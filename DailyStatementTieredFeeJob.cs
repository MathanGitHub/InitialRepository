using LpsHubLib.Ef;
using LpsHubLib.EfClasses.Billing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Utilities;

namespace LpsHubJobs.Jobs.Billing
{
    class DailyStatementTieredFeeJob
    {
        private static ForeignExchangeRealRate[] _dsFxRates;
        private static IEnumerable<ForeignExchangeRealRate> DsFxRates
        {
            get
            {
                if (_dsFxRates == null)
                {
                    using (var dc = new LpsNetBilling2Entities())
                    {
                        var sinceDate = DateTime.Today.AddDays(-100);
                        _dsFxRates = dc.ForeignExchangeRealRates.Where(f => f.FXRateDate >= sinceDate).ToArray();
                    }
                }
                return _dsFxRates;
            }
        }

        const double TimeLimit = 10;
        private static int statementCount = 0;
        private static int statementMapCount = 0;
        private static int transSettlementMapCount = 0;
        private static int reserveCount = 0;
        private static int CarryForwardcount = 0;

        public string CheckMonthlyQualifyCreteria(DateTime calStatementDate, int bankId, out string warningMsg)
        {
            var sb = new StringBuilder();
            var statementstartDate = calStatementDate.AddDays(-calStatementDate.Day + 1).AddMonths(-1);
            var statementEndDate = calStatementDate.AddDays(-calStatementDate.Day + 1);
            var billingCount = 0;
            using (var dc = new LpsNetBilling2Entities() { CommandTimeout = 10800 })
            {
                // get all active billing merchants
                var billingMerchants = dc.LPS_BIL_BillingMerchants.Where(m => m.IsActive).ToArray();
                foreach (var billingMerchant in billingMerchants)
                {
                    // For testing purpose in which we can test against single Merchant 
                    if (billingMerchant.BillingMerchantId != 672)//316
                        continue;

                    var merchantFeeDetails = dc.LPS_BIL_MerchantFeeDetails.Where(r => r.BillingMerchantId == billingMerchant.BillingMerchantId && r.IsActive == true).Distinct().ToList();
                    var banks = merchantFeeDetails.Select(r => new { r.BankId, r.BankConnId }).Distinct().ToList();
                    foreach (var bank in banks)
                    {
                        // For testing purpose only. To be removed soon.
                        if (bank.BankId != bankId)
                            continue;

                        var currencies = merchantFeeDetails.Where(s => s.BankId == bank.BankId).Select(r => new { r.CurrencyCode }).Distinct().ToList();
                        foreach (var currency in currencies)
                        {
                            if (!TieredFeeEnabled(billingMerchant.BillingMerchantId, bank.BankId, currency.CurrencyCode, dc))
                                continue;

                            billingCount += MonthlyQualifyCreteria(billingMerchant, bank.BankId, bank.BankConnId, currency.CurrencyCode, statementstartDate, statementEndDate, calStatementDate.AddDays(-1), dc, sb);

                        }
                    }
                    billingCount++;
                }
            }
            warningMsg = sb.ToString();
            return string.Format("{0} Check Monthly Qualify Creteria.<br />", billingCount);
        }
        private static bool TieredFeeEnabled(int billingMerchantId, int BankConnId, string Currency, LpsNetBilling2Entities dc)
        {
            return dc.LPS_BIL_TF_MerTieredFeeConfigs.Any(s => s.BillingMerchantId == billingMerchantId && s.BankId == BankConnId && s.Currency == Currency && s.IsTieredFeeEnabled == true);
        } 
        private static int MonthlyQualifyCreteria(LPS_BIL_BillingMerchants billingMerchant, int bankId, int bankConnId, string currency, DateTime statementStartDate, DateTime statementEndDate, DateTime monthlyCycleDate, LpsNetBilling2Entities dc, StringBuilder sb)
        {
            var billingCount = 0;
            using (var dtRealTime = new LpsNetBilling2RealTimeEntities() { CommandTimeout = 900 })
            {
                var ThresholdQualifyMaps = dtRealTime.usp_BIL_TF_ThresholdQualifyingCriteria(billingMerchant.BillingMerchantId, bankConnId, currency).ToList();
                var qualifyCriteriaCount = ThresholdQualifyMaps.Select(s => s.QualifyingCriteriaId).Distinct().Count();

                foreach (var ThresholdQualifyMap in ThresholdQualifyMaps)
                {
                    if (dc.LPS_BIL_TF_MerMonthlyQualifyingCriteria.Any(s => s.BillingMerchantId == billingMerchant.BillingMerchantId && s.BankId == bankId && s.BankConnId == bankConnId && s.CurrencyCode == currency && s.MonthlyCycleDate == monthlyCycleDate) && qualifyCriteriaCount == 1)
                        continue;

                    var Transactions = dtRealTime.usp_BIL_TF_CheckMonthlyPurchaseQualifyCriteria(billingMerchant.BillingMerchantId, bankId, bankConnId, currency, ThresholdQualifyMap.TransTypeGroupIds, statementStartDate, statementEndDate).ToList();

                    LPS_BIL_TF_MerMonthlyQualifyingCriteria merMonthlyQualify = new LPS_BIL_TF_MerMonthlyQualifyingCriteria();
                    merMonthlyQualify.BillingMerchantId = billingMerchant.BillingMerchantId;
                    merMonthlyQualify.BankId = bankId;
                    merMonthlyQualify.BankConnId = bankConnId;
                    merMonthlyQualify.CurrencyCode = currency;
                    merMonthlyQualify.Priority = ThresholdQualifyMap.QualifyingCriteriaId == 1 ? 1 :
                                                     dc.LPS_BIL_TF_MerMonthlyQualifyingCriteria.Any(s => s.BillingMerchantId == billingMerchant.BillingMerchantId && s.BankId == bankId && s.BankConnId == bankConnId && s.CurrencyCode == currency && s.MonthlyCycleDate == monthlyCycleDate) ?
                                                 2 : 1;
                    merMonthlyQualify.MonthlyCycleDate = monthlyCycleDate;

                    switch (ThresholdQualifyMap.QualifyingCriteriaId)
                    {
                        case 1: //Transaction Count
                            if (ThresholdQualifyMap.MaxValue != null)
                            {
                                if (ThresholdQualifyMap.MinValue != null)
                                {
                                    if (Transactions.Any(a => a.ProcessCount >= ThresholdQualifyMap.MinValue && a.ProcessCount <= ThresholdQualifyMap.MaxValue))
                                    {
                                        merMonthlyQualify.MerTholdQualiCritId = ThresholdQualifyMap.MerTholdQualiCritId;
                                        dc.AddToLPS_BIL_TF_MerMonthlyQualifyingCriteria(merMonthlyQualify);
                                        dc.SaveChanges();
                                    }
                                }
                                else if (Transactions.Any(a => a.ProcessCount >= ThresholdQualifyMap.MaxValue))
                                {
                                    merMonthlyQualify.MerTholdQualiCritId = ThresholdQualifyMap.MerTholdQualiCritId;
                                    dc.AddToLPS_BIL_TF_MerMonthlyQualifyingCriteria(merMonthlyQualify);
                                    dc.SaveChanges();
                                }
                            }
                            else if (ThresholdQualifyMap.MinValue != null)
                            {
                                if (Transactions.Any(a => a.ProcessCount >= ThresholdQualifyMap.MinValue))
                                {
                                    merMonthlyQualify.MerTholdQualiCritId = ThresholdQualifyMap.MerTholdQualiCritId;
                                    dc.AddToLPS_BIL_TF_MerMonthlyQualifyingCriteria(merMonthlyQualify);
                                    dc.SaveChanges();
                                }
                            }
                            break;
                        case 2://Transaction Volume
                            if (ThresholdQualifyMap.MaxValue != null)
                            {
                                if (ThresholdQualifyMap.MinValue != null)
                                {
                                    if (Transactions.Any(a => a.ProcessAmount >= ThresholdQualifyMap.MinValue && a.ProcessAmount <= ThresholdQualifyMap.MaxValue))
                                    {
                                        merMonthlyQualify.MerTholdQualiCritId = ThresholdQualifyMap.MerTholdQualiCritId;
                                        dc.AddToLPS_BIL_TF_MerMonthlyQualifyingCriteria(merMonthlyQualify);
                                        dc.SaveChanges();
                                    }
                                }
                                else if (Transactions.Any(a => a.ProcessAmount >= ThresholdQualifyMap.MaxValue))
                                {
                                    merMonthlyQualify.MerTholdQualiCritId = ThresholdQualifyMap.MerTholdQualiCritId;
                                    dc.AddToLPS_BIL_TF_MerMonthlyQualifyingCriteria(merMonthlyQualify);
                                    dc.SaveChanges();
                                }
                            }
                            else if (ThresholdQualifyMap.MinValue != null)
                            {
                                if (Transactions.Any(a => a.ProcessAmount >= ThresholdQualifyMap.MinValue))
                                {
                                    merMonthlyQualify.MerTholdQualiCritId = ThresholdQualifyMap.MerTholdQualiCritId;
                                    dc.AddToLPS_BIL_TF_MerMonthlyQualifyingCriteria(merMonthlyQualify);
                                    dc.SaveChanges();
                                }
                            }
                            break;
                        default:
                            break;

                    }
                }


            }

            return billingCount;
        }


        public string CalculateDailyStatements(DateTime calStatementDate, int bankId, out string warningMsg)
        {
            const int maxCalDays = 5; //For valitor
            var sb = new StringBuilder();
            var statementstartDate = DateTime.Now;
            var statementEndDate = DateTime.Now;
            var settleDate = DateTime.Now;
            using (var dc = new LpsNetBilling2Entities() { CommandTimeout = 10800 })
            {
                var billingrunOrders = dc.LPS_BIL_MerchantStmtExecutionOrder.Where(m => m.IsReady && m.BankId == bankId).OrderBy(s => s.RunOrder).Select(s => s.BillingMerchantId).ToList();

                // get ExecutionOrder billing merchants 
                var executOrderBillingMerchants = billingrunOrders.Count() > 0 ?
                                       dc.LPS_BIL_BillingMerchants.Where(m => m.IsActive && billingrunOrders.Contains(m.BillingMerchantId)).ToArray() :
                                       dc.LPS_BIL_BillingMerchants.Where(m => m.IsActive).ToArray(); //all billingmerchants

                foreach (var billingMerchant in executOrderBillingMerchants)
                {
                    // For testing purpose in which we can test against single Merchant 
                    //if (billingMerchant.BillingMerchantId != 538)//316
                    //    continue;

                    /// Get all bank connections for the particular billing merchant from the Fee setup table.
                    var bankConns = dc.LPS_BIL_MerchantFeeDetails.Where(r => r.BillingMerchantId == billingMerchant.BillingMerchantId && r.IsActive == true).Select(r => new { r.BankId, r.BankConnId }).Distinct().ToList();
                    foreach (var bankConn in bankConns)
                    {
                        // For testing purpose only. To be removed soon.
                        if (bankConn.BankId != bankId)
                            continue;

                        //if (!dc.LPS_BIL_BillingMerchantStatementAccount.Any(s => s.BillingMerchantId == billingMerchant.BillingMerchantId && s.BankId == bankConn.BankConnId && s.StatementTypeId == 2))
                        //    continue;

                        #region TimeZone
                        var bankTimezone = dc.LPS_BIL_Cfg_BankTimeZones.Where(b => b.BankConnId == bankConn.BankConnId).FirstOrDefault();
                        if (bankTimezone == null)
                            throw new Exception(string.Format("Error: bankTimezone on {0} are not found.<br/>", bankConn.BankConnId));

                        var timeOffset = new TimeSpan();
                        switch (bankConn.BankConnId)
                        {
                            case 49:
                            case 54:
                            case 78:
                            case 79:
                            case 87:
                            case 88:
                                timeOffset = UtilTimezone.GetBankTimezone(bankTimezone.TimezoneId);
                                settleDate = calStatementDate.AddDays(-1);
                                break;
                            default:
                                //timeOffset = UtilTimezone.GetBankTimezone(bankTimezone.TimezoneId);
                                timeOffset = UtilTimezone.GetBankFixedTimezone(bankTimezone.TimezoneId); //Jay suggested to run with UK Time.12Aug2024. Rollbacked -13Aug2024
                                settleDate = calStatementDate.AddDays(-2);
                                break;
                        }
                        #endregion

                        //var settleDate = calStatementDate.AddDays(-1);
                        #region daily statements

                        if (CheckLogExist(billingMerchant.BillingMerchantId, bankConn.BankConnId, settleDate, 1, dc))
                        {
                            sb.AppendFormat("daily statements already exist for billing merchant={0}, bankconn={1} on {2}. Skipped.<br />", billingMerchant.CompanyName, bankConn.BankConnId, settleDate.ToShortDateString());
                        }
                        else
                        {
                            if (!CheckLogExist(billingMerchant.BillingMerchantId, bankConn.BankConnId, settleDate, 1, dc))
                            {
                                for (var caldate = calStatementDate.AddDays(-maxCalDays); caldate < settleDate; caldate = caldate.AddDays(1))
                                {
                                    if (!CheckLogExist(billingMerchant.BillingMerchantId, bankConn.BankConnId, caldate, 1, dc))
                                    {
                                        //if (!CheckDailyStatementExist(billingMerchant.BillingMerchantId, bankConn.BankConnId, caldate, dc))
                                        //{
                                        //Additional checks on daily statement to make sure Phase 1 job completed per statement date.
                                        var phase1Status = (new BinlistJob()).Phase1JobStatus(caldate, bankId);
                                        if (!string.IsNullOrEmpty(phase1Status))
                                            throw new Exception(phase1Status);

                                        statementstartDate = caldate.AddHours(-(int)bankTimezone.BankTimeOffSetStart);
                                        statementEndDate = statementstartDate.AddDays(1);

                                        ////10Mar2022
                                        //var _firstSunday = UtilDate.GetFirstSundayOfMonth(caldate);
                                        //sb.AppendFormat("First Sunday of Month:{0}.<br />", _firstSunday);
                                        //if (_firstSunday == caldate && (_firstSunday.Month == 4 || _firstSunday.Month == 10))
                                        //{
                                        //    switch (bankConn.BankConnId)
                                        //    {
                                        //        case 49:
                                        //        case 54:
                                        //        case 78:
                                        //        case 79:
                                        //        case 87:
                                        //        case 88:
                                        //            statementstartDate = _firstSunday.Month == 4 ? caldate.AddHours(-7) :
                                        //                                                          _firstSunday.Month == 10 ?
                                        //                                                          caldate.AddHours(-5) : statementstartDate;
                                        //            break;
                                        //        default:
                                        //            break;
                                        //    }
                                        //}

                                        //22Mar2024
                                        var _IsFirstWeekSunday = UtilDate.IsFirstWeekSundayOfMonth(caldate);
                                        if (_IsFirstWeekSunday && (caldate.Month == 4 || caldate.Month == 10))
                                        {
                                            switch (bankConn.BankConnId)
                                            {
                                                case 49:
                                                case 54:
                                                case 78:
                                                case 79:
                                                case 87:
                                                case 88:
                                                    statementstartDate = caldate.Month == 4 ? caldate.AddHours(-7) :
                                                                                                  caldate.Month == 10 ?
                                                                                                  caldate.AddHours(-5) : statementstartDate;
                                                    break;
                                                default:
                                                    break;
                                            }
                                            sb.AppendFormat("First Sunday of Month:{0}.<br />", statementstartDate);
                                        }
                                        statementCount = InsertDailyStatements(billingMerchant, bankConn.BankId, bankConn.BankConnId, caldate, statementstartDate, statementEndDate, bankTimezone != null ? (int)timeOffset.TotalHours : 0, dc, sb);
                                        // insert log
                                        InsertLog(billingMerchant.BillingMerchantId, bankConn.BankConnId, caldate, 1, dc);
                                        InsertLog(billingMerchant.BillingMerchantId, bankConn.BankConnId, caldate, 2, dc);
                                        if (!CheckCarryForwordExist(billingMerchant.BillingMerchantId, bankConn.BankId, bankConn.BankConnId, caldate, dc))
                                            CarryForwardcount = InsertDailyStatementCarryForward(billingMerchant, bankConn.BankId, bankConn.BankConnId, caldate, dc, sb);
                                        //}
                                    }
                                }
                            }

                            statementstartDate = settleDate.AddHours(-(int)bankTimezone.BankTimeOffSetStart);
                            statementEndDate = statementstartDate.AddDays(1);
                            //10Mar2022
                            //var firstSunday = UtilDate.GetFirstSundayOfMonth(settleDate);// This was added for TimeZone Issue 
                            //sb.AppendFormat("First Sunday of Month:{0}.<br />", firstSunday);
                            //if (firstSunday == settleDate && (firstSunday.Month == 4 || firstSunday.Month == 10))
                            //{
                            //    switch (bankConn.BankConnId)
                            //    {
                            //        case 49:
                            //        case 54:
                            //        case 78:
                            //        case 79:
                            //        case 87:
                            //        case 88:
                            //            statementstartDate = firstSunday.Month == 4 ? settleDate.AddHours(-7) :
                            //                                                          firstSunday.Month == 10 ?
                            //                                                          settleDate.AddHours(-5) : statementstartDate;
                            //            break;
                            //        default:
                            //            break;
                            //    }
                            //}

                            //22Mar2024
                            var IsFirstWeekSunday = UtilDate.IsFirstWeekSundayOfMonth(settleDate);
                            if (IsFirstWeekSunday && (settleDate.Month == 4 || settleDate.Month == 10))
                            {
                                switch (bankConn.BankConnId)
                                {
                                    case 49:
                                    case 54:
                                    case 78:
                                    case 79:
                                    case 87:
                                    case 88:
                                        statementstartDate = settleDate.Month == 4 ? settleDate.AddHours(-7) :
                                                                                      settleDate.Month == 10 ?
                                                                                      settleDate.AddHours(-5) : statementstartDate;
                                        break;
                                    default:
                                        break;
                                }
                                sb.AppendFormat("First Sunday of Month:{0}.<br />", statementstartDate);
                            }

                            // insert dailystatement
                            statementCount = InsertDailyStatements(billingMerchant, bankConn.BankId, bankConn.BankConnId, settleDate, statementstartDate, statementEndDate, bankTimezone != null ? (int)timeOffset.TotalHours : 0, dc, sb);
                            // insert log
                            InsertLog(billingMerchant.BillingMerchantId, bankConn.BankConnId, settleDate, 1, dc);
                            InsertLog(billingMerchant.BillingMerchantId, bankConn.BankConnId, settleDate, 2, dc);
                            if (!CheckCarryForwordExist(billingMerchant.BillingMerchantId, bankConn.BankId, bankConn.BankConnId, settleDate, dc))
                                CarryForwardcount = InsertDailyStatementCarryForward(billingMerchant, bankConn.BankId, bankConn.BankConnId, settleDate, dc, sb);
                        }

                        #endregion daily statements
                    }
                    statementCount++;
                }

                // get all active billing merchants
                if (billingrunOrders.Count() != 0)
                {
                    var billingMerchants = dc.LPS_BIL_BillingMerchants.Where(m => m.IsActive).ToArray();
                    foreach (var billingMerchant in billingMerchants)
                    {
                        if (billingrunOrders.Contains(billingMerchant.BillingMerchantId)) //check this billingmerchant already run.
                            continue;
                        // For testing purpose in which we can test against single Merchant 
                        //if (billingMerchant.BillingMerchantId != 538)//316
                        //    continue;

                        /// Get all bank connections for the particular billing merchant from the Fee setup table.
                        var bankConns = dc.LPS_BIL_MerchantFeeDetails.Where(r => r.BillingMerchantId == billingMerchant.BillingMerchantId && r.IsActive == true).Select(r => new { r.BankId, r.BankConnId }).Distinct().ToList();
                        foreach (var bankConn in bankConns)
                        {
                            // For testing purpose only. To be removed soon.
                            if (bankConn.BankId != bankId)
                                continue;

                            //if (!dc.LPS_BIL_BillingMerchantStatementAccount.Any(s => s.BillingMerchantId == billingMerchant.BillingMerchantId && s.BankId == bankConn.BankConnId && s.StatementTypeId == 2))
                            //    continue;

                            #region TimeZone
                            var bankTimezone = dc.LPS_BIL_Cfg_BankTimeZones.Where(b => b.BankConnId == bankConn.BankConnId).FirstOrDefault();
                            if (bankTimezone == null)
                                throw new Exception(string.Format("Error: bankTimezone on {0} are not found.<br/>", bankConn.BankConnId));

                            var timeOffset = new TimeSpan();
                            switch (bankConn.BankConnId)
                            {
                                case 49:
                                case 54:
                                case 78:
                                case 79:
                                case 87:
                                case 88:
                                    timeOffset = UtilTimezone.GetBankTimezone(bankTimezone.TimezoneId);
                                    settleDate = calStatementDate.AddDays(-1);
                                    break;
                                default:
                                    //timeOffset = UtilTimezone.GetBankTimezone(bankTimezone.TimezoneId);
                                    timeOffset = UtilTimezone.GetBankFixedTimezone(bankTimezone.TimezoneId); //Jay suggested to run with UK Time.12Aug2024. Rollbacked -13Aug2024
                                    settleDate = calStatementDate.AddDays(-2);
                                    break;
                            }
                            #endregion

                            //var settleDate = calStatementDate.AddDays(-1);
                            #region daily statements

                            if (CheckLogExist(billingMerchant.BillingMerchantId, bankConn.BankConnId, settleDate, 1, dc))
                            {
                                sb.AppendFormat("daily statements already exist for billing merchant={0}, bankconn={1} on {2}. Skipped.<br />", billingMerchant.CompanyName, bankConn.BankConnId, settleDate.ToShortDateString());
                            }
                            else
                            {
                                if (!CheckLogExist(billingMerchant.BillingMerchantId, bankConn.BankConnId, settleDate, 1, dc))
                                {
                                    for (var caldate = calStatementDate.AddDays(-maxCalDays); caldate < settleDate; caldate = caldate.AddDays(1))
                                    {
                                        if (!CheckLogExist(billingMerchant.BillingMerchantId, bankConn.BankConnId, caldate, 1, dc))
                                        {
                                            //if (!CheckDailyStatementExist(billingMerchant.BillingMerchantId, bankConn.BankConnId, caldate, dc))
                                            //{
                                            //Additional checks on daily statement to make sure Phase 1 job completed per statement date.
                                            var phase1Status = (new BinlistJob()).Phase1JobStatus(caldate, bankId);
                                            if (!string.IsNullOrEmpty(phase1Status))
                                                throw new Exception(phase1Status);

                                            statementstartDate = caldate.AddHours(-(int)bankTimezone.BankTimeOffSetStart);
                                            statementEndDate = statementstartDate.AddDays(1);

                                            ////10Mar2022
                                            //var _firstSunday = UtilDate.GetFirstSundayOfMonth(caldate);
                                            //sb.AppendFormat("First Sunday of Month:{0}.<br />", _firstSunday);
                                            //if (_firstSunday == caldate && (_firstSunday.Month == 4 || _firstSunday.Month == 10))
                                            //{
                                            //    switch (bankConn.BankConnId)
                                            //    {
                                            //        case 49:
                                            //        case 54:
                                            //        case 78:
                                            //        case 79:
                                            //        case 87:
                                            //        case 88:
                                            //            statementstartDate = _firstSunday.Month == 4 ? caldate.AddHours(-7) :
                                            //                                                          _firstSunday.Month == 10 ?
                                            //                                                          caldate.AddHours(-5) : statementstartDate;
                                            //            break;
                                            //        default:
                                            //            break;
                                            //    }
                                            //}

                                            //22Mar2024
                                            var _IsFirstWeekSunday = UtilDate.IsFirstWeekSundayOfMonth(caldate);
                                            if (_IsFirstWeekSunday && (caldate.Month == 4 || caldate.Month == 10))
                                            {
                                                switch (bankConn.BankConnId)
                                                {
                                                    case 49:
                                                    case 54:
                                                    case 78:
                                                    case 79:
                                                    case 87:
                                                    case 88:
                                                        statementstartDate = caldate.Month == 4 ? caldate.AddHours(-7) :
                                                                                                      caldate.Month == 10 ?
                                                                                                      caldate.AddHours(-5) : statementstartDate;
                                                        break;
                                                    default:
                                                        break;
                                                }
                                                sb.AppendFormat("First Sunday of Month:{0}.<br />", statementstartDate);
                                            }
                                            statementCount = InsertDailyStatements(billingMerchant, bankConn.BankId, bankConn.BankConnId, caldate, statementstartDate, statementEndDate, bankTimezone != null ? (int)timeOffset.TotalHours : 0, dc, sb);
                                            // insert log
                                            InsertLog(billingMerchant.BillingMerchantId, bankConn.BankConnId, caldate, 1, dc);
                                            InsertLog(billingMerchant.BillingMerchantId, bankConn.BankConnId, caldate, 2, dc);
                                            if (!CheckCarryForwordExist(billingMerchant.BillingMerchantId, bankConn.BankId, bankConn.BankConnId, caldate, dc))
                                                CarryForwardcount = InsertDailyStatementCarryForward(billingMerchant, bankConn.BankId, bankConn.BankConnId, caldate, dc, sb);
                                            //}
                                        }
                                    }
                                }

                                statementstartDate = settleDate.AddHours(-(int)bankTimezone.BankTimeOffSetStart);
                                statementEndDate = statementstartDate.AddDays(1);
                                //10Mar2022
                                //var firstSunday = UtilDate.GetFirstSundayOfMonth(settleDate);// This was added for TimeZone Issue 
                                //sb.AppendFormat("First Sunday of Month:{0}.<br />", firstSunday);
                                //if (firstSunday == settleDate && (firstSunday.Month == 4 || firstSunday.Month == 10))
                                //{
                                //    switch (bankConn.BankConnId)
                                //    {
                                //        case 49:
                                //        case 54:
                                //        case 78:
                                //        case 79:
                                //        case 87:
                                //        case 88:
                                //            statementstartDate = firstSunday.Month == 4 ? settleDate.AddHours(-7) :
                                //                                                          firstSunday.Month == 10 ?
                                //                                                          settleDate.AddHours(-5) : statementstartDate;
                                //            break;
                                //        default:
                                //            break;
                                //    }
                                //}

                                //22Mar2024
                                var IsFirstWeekSunday = UtilDate.IsFirstWeekSundayOfMonth(settleDate);
                                if (IsFirstWeekSunday && (settleDate.Month == 4 || settleDate.Month == 10))
                                {
                                    switch (bankConn.BankConnId)
                                    {
                                        case 49:
                                        case 54:
                                        case 78:
                                        case 79:
                                        case 87:
                                        case 88:
                                            statementstartDate = settleDate.Month == 4 ? settleDate.AddHours(-7) :
                                                                                          settleDate.Month == 10 ?
                                                                                          settleDate.AddHours(-5) : statementstartDate;
                                            break;
                                        default:
                                            break;
                                    }
                                    sb.AppendFormat("First Sunday of Month:{0}.<br />", statementstartDate);
                                }

                                // insert dailystatement
                                statementCount = InsertDailyStatements(billingMerchant, bankConn.BankId, bankConn.BankConnId, settleDate, statementstartDate, statementEndDate, bankTimezone != null ? (int)timeOffset.TotalHours : 0, dc, sb);
                                // insert log
                                InsertLog(billingMerchant.BillingMerchantId, bankConn.BankConnId, settleDate, 1, dc);
                                InsertLog(billingMerchant.BillingMerchantId, bankConn.BankConnId, settleDate, 2, dc);
                                if (!CheckCarryForwordExist(billingMerchant.BillingMerchantId, bankConn.BankId, bankConn.BankConnId, settleDate, dc))
                                    CarryForwardcount = InsertDailyStatementCarryForward(billingMerchant, bankConn.BankId, bankConn.BankConnId, settleDate, dc, sb);
                            }

                            #endregion daily statements
                        }
                        statementCount++;
                    }
                }
            }
            warningMsg = sb.ToString();
            return string.Format("{0} daily statements generated.<br />{1} reserves generated.<br />", statementCount, reserveCount);
        }
        private static bool CheckLogExist(int billingMerchantId, int bankConnId, DateTime calDate, int calType, LpsNetBilling2Entities dc)
        {
            // calType:1 -> daily statement;
            return dc.LPS_BIL_DailyCalculationLogs.Any(l => l.BillingMerchantId == billingMerchantId && l.BankConnId == bankConnId && l.LogDate == calDate && l.CalType == calType);
        }

        private static void InsertLog(int billingMerchantId, int bankConnId, DateTime reserveDate, int calType, LpsNetBilling2Entities dc)
        {
            var dclog = new LPS_BIL_DailyCalculationLogs
            {
                LogDate = reserveDate,
                BillingMerchantId = billingMerchantId,
                BankConnId = bankConnId,
                CalType = calType
            };

            dc.LPS_BIL_DailyCalculationLogs.AddObject(dclog);
            dc.SaveChanges();
        }

        private static bool CheckDailyStatementExist(int billingMerchantId, int BankConnId, DateTime statementDate, LpsNetBilling2Entities dc)
        {
            return dc.LPS_BIL_MerchantDailyStatements.Any(s => s.BankConnId == BankConnId && s.BillingMerchantId == billingMerchantId && s.StatementDate == statementDate);
        }
        private static bool CheckDailyStatementExist(int billingMerchantId, int BankConnId, string merchantsite, string currency, DateTime statementDate, LpsNetBilling2Entities dc)
        {
            return dc.LPS_BIL_MerchantDailyStatements.Any(s => s.BillingMerchantId == billingMerchantId && s.MerchantSiteId == merchantsite && s.BankConnId == BankConnId && s.CurrencyCode == currency && s.StatementDate == statementDate);
        }
        private static bool CheckCarryForwordExist(int billingMerchantId, int BankId, int BankConnId, DateTime statementDate, LpsNetBilling2Entities dc)
        {
            return dc.LPS_BIL_MerchantDailyStatmentCarryForward.Any(s => s.BankConnId == BankConnId && s.BillingMerchantId == billingMerchantId && s.StatementDate == statementDate);
        }
        private static bool DailyStatementEnabled(int billingMerchantId, int BankConnId, string Currency, LpsNetBilling2Entities dc)
        {
            return dc.LPS_BIL_BillingMerchantConfig.Any(s => s.BillingMerchantId == billingMerchantId && s.BankId == BankConnId && s.Currency == Currency && s.IsDailyStatementEnabled == true);
        }
        public bool CheckDailyStatementEnabled(int[] billingMerchantIds, int BankConnId)
        {
            return new LpsNetBilling2Entities().LPS_BIL_BillingMerchantConfig.Any(s => billingMerchantIds.Contains(s.BillingMerchantId) && s.BankId == BankConnId && s.IsDailyStatementEnabled == true);
        }
        private static int InsertDailyStatementCarryForward(LPS_BIL_BillingMerchants billingMerchant, int bankId, int bankConnId, DateTime statementDate, LpsNetBilling2Entities dc, StringBuilder sb)
        {

            // get currencies from merchant Fee details            
            var currencies = dc.LPS_BIL_MerchantFeeDetails.Where(r => r.BillingMerchantId == billingMerchant.BillingMerchantId && r.BankConnId == bankConnId && r.IsActive == true).Select(r => r.CurrencyCode).Distinct().ToList();
            if (!currencies.Any())
                return CarryForwardcount;
            //Loop through Currency
            foreach (var currency in currencies)
            {

                //New View created for this "View_BIL_DailyStatements"- View_BIL_DailyStatementsforMultipleBankconn 14FEb2023
                var dailyStatements = dc.View_BIL_DailyStatementsforMultipleBankconn_Job.Where(p => p.BillingMerchantId == billingMerchant.BillingMerchantId && p.BankConnId == bankConnId && p.CurrencyCode == currency && p.StatementDate >= statementDate && p.StatementDate <= statementDate);
                var dailyStatementsWithoutBankConn = dc.View_BIL_DailyStatements_Job.Where(p => p.BillingMerchantId == billingMerchant.BillingMerchantId && p.BankId == bankId && p.CurrencyCode == currency && p.StatementDate >= statementDate && p.StatementDate <= statementDate);

                var CFLimits = dc.LPS_BIL_CarryForwardLimits.Where(p => p.BillingMerchantId == billingMerchant.BillingMerchantId && p.BankId == bankId && p.CurrencyCode == currency).FirstOrDefault();
                var SettlementAmount = dailyStatements != null ? dailyStatements.Sum(s => s.AvailableBalance) : 0;
                var SettlementAmountWithoutBankConn = dailyStatementsWithoutBankConn != null ? dailyStatementsWithoutBankConn.Sum(s => s.AvailableBalance) : 0;
                var previousStmtDate = statementDate.AddDays(-1);


                if (CFLimits != null)
                {
                    if (CFLimits.LimitAmount == (decimal)0.00)
                        continue;

                    //BroughtForward
                    var CarryDetails = dc.LPS_BIL_MerchantDailyStatmentCarryForward.Where(p => p.BillingMerchantId == billingMerchant.BillingMerchantId && p.BankConnId == bankConnId && p.Currency == currency && p.StatementDate == previousStmtDate && p.TobeCarried == true);
                    if (CarryDetails.Count() > 0)
                    {
                        var dailystatementId = dc.LPS_BIL_MerchantDailyStatements.Where(p => p.BillingMerchantId == billingMerchant.BillingMerchantId && p.BankConnId == bankConnId && p.CurrencyCode == currency && p.StatementDate >= previousStmtDate && p.StatementDate <= previousStmtDate).Select(p => (int?)p.DailyStatementId).FirstOrDefault();

                        var carryForward = new LPS_BIL_MerchantDailyStatmentCarryForward
                        {
                            StatementDate = statementDate,
                            BillingMerchantId = billingMerchant.BillingMerchantId,
                            BankId = bankId,
                            BankConnId = bankConnId,
                            Currency = currency,
                            BroughtForward = CarryDetails.Sum(s => s.SettlementAmount),
                            BroughtFromStatementId = dailystatementId,
                            TobeCarried = (CFLimits.LimitAmount != (decimal)0.00) ? SettlementAmountWithoutBankConn < CFLimits.LimitAmount ? true : false : false,
                            SettlementAmount = SettlementAmount,
                            IsPaid = false,
                            IsVisible = false
                        };
                        dc.LPS_BIL_MerchantDailyStatmentCarryForward.AddObject(carryForward);
                        dc.SaveChanges();

                        CarryForwardcount++;
                    }
                    else
                    {
                        var carryForward = new LPS_BIL_MerchantDailyStatmentCarryForward
                        {
                            StatementDate = statementDate,
                            BillingMerchantId = billingMerchant.BillingMerchantId,
                            BankId = bankId,
                            BankConnId = bankConnId,
                            Currency = currency,
                            BroughtFromStatementId = null,
                            BroughtForward = (decimal?)0.00,
                            SettlementAmount = SettlementAmount,
                            TobeCarried = (CFLimits.LimitAmount != (decimal)0.00) ? SettlementAmountWithoutBankConn < CFLimits.LimitAmount ? true : false : false,
                            IsPaid = false,
                            IsVisible = false
                        };
                        dc.LPS_BIL_MerchantDailyStatmentCarryForward.AddObject(carryForward);
                        dc.SaveChanges();
                        CarryForwardcount++;
                    }

                    //13Mar2023
                    var bankConns = dc.LPS_BIL_MerchantFeeDetails.Where(r => r.BillingMerchantId == billingMerchant.BillingMerchantId && r.BankId == bankId && r.CurrencyCode == currency && r.IsActive == true).Select(r => r.BankConnId).Count();

                    if (bankConns > 1)
                    {
                        var ExistingCarryDetails = dc.LPS_BIL_MerchantDailyStatmentCarryForward.Where(p => p.BillingMerchantId == billingMerchant.BillingMerchantId && p.BankId == bankId && p.Currency == currency && p.StatementDate == statementDate && p.TobeCarried == true).ToList();
                        foreach (var exist in ExistingCarryDetails)
                        {
                            exist.TobeCarried = (CFLimits.LimitAmount != (decimal)0.00) ? SettlementAmountWithoutBankConn < CFLimits.LimitAmount ? true : false : false;
                            if (!exist.TobeCarried)
                            {
                                exist.BroughtFromStatementId = null;
                                exist.BroughtForward = (decimal?)0.00;
                            }
                            dc.SaveChanges();
                        }
                    }
                }

            }
            return CarryForwardcount;
        }

        private static int InsertDailyStatements(LPS_BIL_BillingMerchants billingMerchant, int bankId, int bankConnId, DateTime statementDate, DateTime statementStartDate, DateTime statementEndDate, int timeOffset, LpsNetBilling2Entities dc, StringBuilder sb)
        {
            // var statementCount = 0;
            var feeCount = 0;
            var IsSettledPerSite = 1;
            var ignoreDuplicateSettlement = new List<dynamic>();
            //var feeTypeIdsDone = new List<int>();
            using (var dtRealTime = new LpsNetBilling2RealTimeEntities() { CommandTimeout = 900 })
            {
                //Get site, currency, groupid and bank id for the particular billing merchant
                var transSites = dtRealTime.usp_BIL_GetSitesFromTransByBillingMerchant(billingMerchant.BillingMerchantId, statementDate).ToList();

                //16Feb2023 -This new logic added for Monthly account fees even if the client doesn't get started processing           
                var feeSites = dtRealTime.usp_BIL_GetSitesFromFeesByBillingMerchant(billingMerchant.BillingMerchantId, bankConnId).Select(s => new usp_BIL_GetSitesFromTransByBillingMerchant_Result
                {
                    Merchant_User_Id = s.Merchant_User_Id,
                    MerchantGroup = s.MerchantGroupId,
                    BankConnId = (int)s.BankConnId,
                    BankId = s.BankId,
                    CurrencyCode = s.CurrencyCode
                }).ToList();

                var merSites = transSites.Union(feeSites).Distinct();

                var efStatementFee = new EfStatementFee();
                foreach (var site in merSites)
                {
                    // solve multi-bank issue
                    if (site.BankConnId != bankConnId)
                        continue;

                    if (!DsFxRates.Any(fxr => fxr.FXRateDate == statementDate))
                        throw new Exception(string.Format("Error: foreign exchange rates on {0} are not found.<br/>", statementDate.ToShortDateString()));

                    // get currencies from merchant Fee details            
                    var currencies = dc.LPS_BIL_MerchantFeeDetails.Where(r => r.BillingMerchantId == billingMerchant.BillingMerchantId && r.BankConnId == bankConnId && r.IsActive == true).Select(r => r.CurrencyCode).Distinct().ToList();
                    if (!currencies.Any())
                        return statementCount;
                    //Loop through Currency
                    foreach (var currency in currencies)
                    {
                        // solve multi-currency issue
                        //if (site.CurrencyCode != currency)
                        //    throw new Exception(string.Format("There is no Fee setup for the particular Currency: {0}.<br/>", site.CurrencyCode));

                        if (site.CurrencyCode != currency)
                            continue;

                        if (!DailyStatementEnabled(billingMerchant.BillingMerchantId, site.BankId, site.CurrencyCode, dc))
                            continue;
                        //added 17Feb2023
                        if (CheckDailyStatementExist(billingMerchant.BillingMerchantId, site.BankConnId, site.Merchant_User_Id, site.CurrencyCode, statementDate, dc))
                            continue;
                        if (currency.Length > 3)
                            sb.AppendFormat("currency has an ending space(SMId={0}, curr={1}.<br/>", billingMerchant.BillingMerchantId, currency);

                        var merFeedetailId = dc.LPS_BIL_MerchantFeeDetails.Where(r => r.BillingMerchantId == billingMerchant.BillingMerchantId && r.BankConnId == bankConnId && r.CurrencyCode == currency && r.IsActive == true && r.FeeLevelId == 1).OrderByDescending(s => s.MerFeeDetailId).Select(r => r.MerFeeDetailId).FirstOrDefault();
                        if (merFeedetailId == 0)
                            throw new Exception(string.Format("There is no Client Fee setup for the particular Merchant{0}.<br />", billingMerchant.CompanyName));

                        #region Step 1:Insert into Daily statement table  basic details 

                        var dailyStatement = new LPS_BIL_MerchantDailyStatements
                        {
                            StatementDate = statementDate,
                            BillingMerchantId = billingMerchant.BillingMerchantId,
                            MerchantGroupId = site.MerchantGroup,
                            MerchantSiteId = site.Merchant_User_Id,
                            CurrencyCode = currency,
                            BankId = bankId,
                            BankConnId = bankConnId,
                            ReserveHeld = (decimal)0.00,        // Value through update
                            ReserveRelease = (decimal)0.00,     // Value through update
                            ManualAdjustCredit = (decimal)0.00, // Value through update
                            ManualAdjustDebit = (decimal)0.00, // Value through update                             
                            PaymentPaid = (decimal)0.00,        // Value through update
                            BODBalance = (decimal)0.00,         // Value through update 
                            EODBalance = (decimal)0.00,         // Value through update
                            CumulativeReserve = (decimal)0.00,  // Value through update      
                            CumulativeBalance = (decimal)0.00,  // Value through update
                            AvailableBalance = (decimal)0.00    // Value through update
                        };
                        dc.LPS_BIL_MerchantDailyStatements.AddObject(dailyStatement);
                        dc.SaveChanges();

                        #endregion

                        #region Step 2:Insert into Daily statement table Total Trans Counts 
                        foreach (var transType in dc.LPS_BIL_Cfg_TransTypeGroups.ToList())
                        {
                            var dailytransTotal = EfDailyStatementTieredFee.CalDailyStatementSummary(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankConnId, statementStartDate, statementEndDate, timeOffset, sb, dailyStatement.DailyStatementId, transType.TransTypeId, transType.TransGroupId, transType.TransTypeGroupId);
                            if (dailytransTotal.ProcessCount != 0)
                            {
                                // insert to db
                                dc.LPS_BIL_MerchantDailyStatementSummary.AddObject(dailytransTotal);
                                dc.SaveChanges();
                            }

                        }
                        #endregion

                        #region Step 3:Insert into Daily statement table Trans Category Counts

                        foreach (var transTypeCat in dc.LPS_BIL_Cfg_TransTypeCategories.Where(b => b.IsActive == true).ToList())
                        {
                            foreach (var transType in dc.LPS_BIL_Cfg_TransTypeGroups.ToList())
                            {
                                var transTypeGroupAndTransCats = dc.LPS_BIL_Cfg_FeeTypeWithTransTypeGroupAndTransCat.Where(m => m.TransTypeGroupId == transType.TransTypeGroupId && m.TransTypeCatId == transTypeCat.TransTypeCatId).Select(m => new { m.TransTypeGroupId, m.TransTypeCatId }).FirstOrDefault();
                                if (transTypeGroupAndTransCats == null)
                                    continue;
                                if (transType.TransTypeGroupId != transTypeGroupAndTransCats.TransTypeGroupId)
                                    continue;
                                var dailytransCatTotal = EfDailyStatementTieredFee.CalDailyStatementSummaryTransByCategory(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankConnId, statementStartDate, statementEndDate, timeOffset, sb, dailyStatement.DailyStatementId, transType.TransTypeId, transType.TransGroupId, transTypeGroupAndTransCats.TransTypeGroupId, transTypeGroupAndTransCats.TransTypeCatId);
                                if (dailytransCatTotal.ProcessCount != 0)
                                {
                                    dc.LPS_BIL_MerchantDailyStatementSummaryByCategory.AddObject(dailytransCatTotal);
                                    dc.SaveChanges();
                                }
                            }

                        }
                        #endregion

                        #region Step 4:Insert into Reserve table              

                        //var settledTransAmount = dc.usp_BIL_CalDailyStatementOfPurchase(billingMerchant.BillingMerchantId, site.Merchant_User_Id, site.MerchantGroup, bankConnId, currency, 0, null, null, null, null, "1,2", "0", statementStartDate, statementEndDate, timeOffset).ToList();
                        //if (settledTransAmount.Sum(s => s.ActualTransAmount) > 0)
                        //{
                        //    if (!EfMerchantReserve.CheckReserveExist(dc, billingMerchant.BillingMerchantId, site.Merchant_User_Id, bankConnId, currency, dailyStatement.DailyStatementId, statementDate))
                        //    {
                        //        var reserve = EfMerchantReserve.InsertReserve(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, bankConnId, currency, dailyStatement.DailyStatementId, statementDate, settledTransAmount.Sum(s => s.ActualTransAmount));
                        //        if (reserve != null)
                        //        {
                        //            dc.LPS_BIL_MerchantDailyReserves.AddObject(reserve);
                        //            dc.SaveChanges();
                        //        }
                        //        reserveCount++;                           
                        //    }
                        //}
                        if (!EfMerchantReserve.CheckReserveExist(dc, billingMerchant.BillingMerchantId, site.Merchant_User_Id, bankConnId, currency, dailyStatement.DailyStatementId, statementDate))
                        {
                            //Dailystatement Summary
                            var reserveTransAmount = dtRealTime.usp_BIL_CalDailyStatementOfPurchase(billingMerchant.BillingMerchantId, site.Merchant_User_Id, site.MerchantGroup, bankConnId, currency, 0, null, null, null, null, "1", "0", statementStartDate, statementEndDate, timeOffset, 1).ToList();
                            if (reserveTransAmount.Sum(s => s.ActualTransAmount) > 0)
                            {

                                var reserve = EfMerchantReserve.InsertReserve(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, bankConnId, currency, dailyStatement.DailyStatementId, statementDate, reserveTransAmount.Sum(s => s.ActualTransAmount));
                                if (reserve != null)
                                {
                                    dc.LPS_BIL_MerchantDailyReserves.AddObject(reserve);
                                    dc.SaveChanges();
                                }
                                reserveCount++;

                            }

                            //Dailystatement Summary by category
                            //var transTypeGroupAndTransCats = dc.LPS_BIL_Cfg_FeeTypeWithTransTypeGroupAndTransCat.Where(m => m.TransTypeGroupId == 1).Select(m => new { m.TransTypeCatId }).Distinct().ToList();
                            //foreach (var transType in transTypeGroupAndTransCats)
                            //{
                            //    var settledTransAmount = dc.usp_BIL_CalDailyStatementOfPurchase(billingMerchant.BillingMerchantId, site.Merchant_User_Id, site.MerchantGroup, bankConnId, currency, 0, null, null, null, null, "1", transType.TransTypeCatId.ToString(), statementStartDate, statementEndDate, timeOffset).ToList();
                            //    if (settledTransAmount.Sum(s => s.ActualTransAmount) > 0)
                            //    {
                            //        var reserve = EfMerchantReserve.InsertReserve(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, bankConnId, currency, dailyStatement.DailyStatementId, statementDate, settledTransAmount.Sum(s => s.ActualTransAmount));
                            //        if (reserve != null)
                            //        {
                            //            dc.LPS_BIL_MerchantDailyReserves.AddObject(reserve);
                            //            dc.SaveChanges();
                            //        }
                            //        reserveCount++;
                            //    }
                            //}
                        }
                        else
                        {
                            sb.AppendFormat("reserve statements already exist for billing merchant={0},merchant_site={1} bankconn={2} on {3}. Skipped.<br />", billingMerchant.CompanyName, site.Merchant_User_Id, bankConnId, statementDate.ToShortDateString());
                        }
                        #endregion

                        #region Step 5:Client Fee

                        //Monthly
                        var startOfMonth = statementDate.AddDays(-statementDate.Day + 1);
                        var endOfMonth = startOfMonth.AddMonths(1).AddDays(-1);

                        //PreviousMonth 
                        var startOfPreviousMonth = startOfMonth.AddMonths(-1);
                        var endOfPreviousMonth = startOfMonth.AddDays(-1);

                        //if (dtRealTime.View_BIL_BankDailyStatements_Main.Any(s => s.BillingMerchantId == billingMerchant.BillingMerchantId && s.BankId == bankConnId && s.CurrencyCode == currency && s.StatementDate >= startOfPreviousMonth && s.StatementDate <= endOfPreviousMonth && s.TotalCounts > 0))
                        //{
                        //get Feetype from merchant Fee details
                        var feeTypeIds = dc.LPS_BIL_TF_ClientFeeRates.Where(r => r.MerFeeDetailId == merFeedetailId).Select(r => r.FeeTypeId).Distinct().ToList();
                        if (feeTypeIds == null || feeTypeIds.Count == 0)
                            throw new Exception(string.Format("There is no FeeType setup for the particular Merchant {0}.<br />", billingMerchant.CompanyName));

                        #region  Step 5(a): Insert into MerchantFeeRates table with actual calculations.

                        foreach (var feeTypeId in feeTypeIds)
                        {

                            feeCount += EfDailyStatementTieredFee.CalDailyStatementClientFeeCount(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankId, bankConnId, merFeedetailId, feeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                        }
                        //}
                        #endregion

                        #region  Step 5(b): Insert into Merchant Billing Fee Rates table with actual calculations.

                        var tieredPricing = dtRealTime.usp_BIL_TF_ThresholdTieredPricing(billingMerchant.BillingMerchantId, bankConnId, currency, 0).Select(s => s.MerTieredPriceId).ToList();


                        // Get list of billing fee types for this billing merchant and based on this retrive the billing method id to check whether its Blended or Blended interchange
                        var billingMethodFeeTypeIds = dc.LPS_BIL_TF_ClientBillingFeeRates.Where(r => r.MerFeeDetailId == merFeedetailId && tieredPricing.Contains(r.MerTieredPriceId)).Select(r => r.FeeTypeId).Distinct().ToList();
                        // To check empty or null records
                        if (billingMethodFeeTypeIds == null || billingMethodFeeTypeIds.Count == 0)
                            throw new Exception(string.Format("There is no Billing FeeType setup for this Merchant {0}.<br />", billingMerchant.CompanyName));

                        // Loop through billing fee types to find billing method id.
                        foreach (var billingmethodfeetypeid in billingMethodFeeTypeIds)
                        {
                            // Retrieve the Fee Group of the fee type
                            var feeGroupId = dc.LPS_BIL_Cfg_FeeTypes.Where(t => t.FeeTypeId == billingmethodfeetypeid).Select(t => t.FeeGroupId).FirstOrDefault();

                            // Retrieve the actual billing Method id for proceed further
                            var billingMethodId = dc.LPS_BIL_Cfg_BillingFeeTypes.Where(t => t.FeeGroupId == feeGroupId).Select(t => t.BillingFeeTypeId).FirstOrDefault();
                            switch (billingMethodId)
                            {

                                //usp_BIL_GetCardDetails replaced usp_BIL_GetCardDetailsfromMultipleBankConn 14Feb2023
                                case 4:  //will remove once case 4 worked.
                                    #region Step 5(b) Sec 1 : Blended Rate

                                    var BinListData = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardTransType = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower() }).Distinct().ToList();
                                    foreach (var binlistdata in BinListData)
                                    {
                                        //get card MapId
                                        var cardMaps = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardMapId });

                                        //var cardbrand = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower() }).Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardBrand).FirstOrDefault();
                                        //var cardtype = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower() }).Where(m => m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardType).FirstOrDefault();
                                        //var cardlevel = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower() }).Where(m => m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardLevel).FirstOrDefault();

                                        //if (!string.IsNullOrEmpty(cardbrand))
                                        //    cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase));
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardBrand == "Unknown");
                                        //if (!string.IsNullOrEmpty(cardtype))
                                        //    cardMaps = cardMaps.Where(m => m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase));
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardType == "Unknown");
                                        //if (!string.IsNullOrEmpty(cardlevel))
                                        //    cardMaps = cardMaps.Where(m => m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardLevel == "Unknown");

                                        cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase) &&
                                                                        m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase) &&
                                                                        m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));//27Jun2023

                                        var cardmapId = cardMaps.Select(s => (int)s.CardMapId).FirstOrDefault();
                                        cardmapId = cardmapId == 0 ? 13 : cardmapId;
                                        //16Aug2021
                                        var BinData = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardBrand, s.CardTransType, s.CardLevel }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel).Select(s => new { s.CardBrand, s.CardTransType, s.CardLevel }).FirstOrDefault();
                                        if (cardmapId == 13)
                                        {
                                            //23Nov2021
                                            var BinLists = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.Bin }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel).Select(s => s.Bin).Distinct().ToArray();
                                            sb.AppendFormat(string.Format("No Card Mapping for this <b> Bin: [{0}] ,  Brand: {1} ,  Type: {2} , Level: {3} </b>on Client Fees.<br />", BinLists != null ? string.Join(",", BinLists) : null, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel));
                                        }
                                        // get card maps from Merchant billing fee Rates table                         
                                        var cardMapIds = dc.LPS_BIL_TF_ClientBillingFeeRates.Where(r => r.MerFeeDetailId == merFeedetailId && r.CardMapId == cardmapId && r.FeeTypeId == billingmethodfeetypeid).Select(r => r.CardMapId).Distinct().ToList();
                                        if (cardMapIds == null || cardMapIds.Count == 0)
                                        {
                                            sb.AppendFormat(string.Format("There is no Client Fee Type setup for this Card Map ( Brand: {0} ,  Type: {1} , Level: {2} , Billing Merchant:{3} ).<br />", BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, billingMerchant.CompanyName));
                                            //throw new Exception(string.Format("No Card Mapping for this  Brand:{0},  Type:{1}, Level:{2}.", binlistdata.CardBrand, binlistdata.CardTransType, binlistdata.CardLevel));
                                            cardmapId = 13;
                                        }

                                        var billingFeeTypeIds = dc.LPS_BIL_TF_ClientBillingFeeRates.Where(r => r.CardMapId == cardmapId && r.MerFeeDetailId == merFeedetailId && r.FeeTypeId == billingmethodfeetypeid).Select(r => r.FeeTypeId).Distinct().ToList();
                                        //This is added for Interchange Fee
                                        if (billingFeeTypeIds.Contains(36) || billingFeeTypeIds.Contains(37))
                                            billingFeeTypeIds.Add(30);

                                        if (billingFeeTypeIds == null || billingFeeTypeIds.Count == 0)
                                            throw new Exception(string.Format("There is no Client Fee Type setup for this  Merchant {0} and cardMap {1}.<br />", billingMerchant.CompanyName, cardmapId));
                                        foreach (var billingFeeTypeId in billingFeeTypeIds)
                                        {
                                            feeCount += EfDailyStatementTieredFee.CalDailyStatementClientBillingFeeCount(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankConnId, merFeedetailId, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, cardmapId, billingFeeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                                        }

                                    }
                                    #endregion
                                    break;
                                case 3:
                                    #region Step 5(b) Sec 2 : Blended Interchange
                                    var binlistwithLocation = new List<Tuple<string, string, string, string, int>>();//14Jul2022
                                    var BinListIntraData = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardTransType = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.BinCountry }).Distinct().ToList();
                                    foreach (var binlistdata in BinListIntraData)
                                    {
                                        var binCountry = string.IsNullOrEmpty(binlistdata.BinCountry) ? "GB" : binlistdata.BinCountry; //30Aug2022
                                                                                                                                       //Added
                                        var binLocation = dc.usp_BIL_InterchangMappingDetails(billingMerchant.MerchantId, billingMerchant.BillingMerchantId, binCountry).FirstOrDefault();

                                        //get card MapId
                                        var cardMaps = dc.LPS_BIL_Cfg_CardLevelIntraGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardLocation, s.CardMapId });

                                        //var cardbrand = dc.LPS_BIL_Cfg_CardLevelIntraGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower() }).Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardBrand).FirstOrDefault();
                                        //var cardtype = dc.LPS_BIL_Cfg_CardLevelIntraGrouping.Select(s => new { CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower() }).Where(m => m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardType).FirstOrDefault();
                                        //var cardlevel = dc.LPS_BIL_Cfg_CardLevelIntraGrouping.Select(s => new { CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower() }).Where(m => m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardLevel).FirstOrDefault();
                                        //var cardlocation = binLocation != null ? dc.LPS_BIL_Cfg_CardLevelIntraGrouping.Where(m => m.CardLocation.Equals(binLocation.CardLocation, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardLocation).FirstOrDefault()
                                        //                                          : null;
                                        //if (!string.IsNullOrEmpty(cardbrand))
                                        //    cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase));
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardBrand == "Unknown");
                                        //if (!string.IsNullOrEmpty(cardtype))
                                        //    cardMaps = cardMaps.Where(m => m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase));
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardType == "Unknown");
                                        //if (!string.IsNullOrEmpty(cardlevel))
                                        //    cardMaps = cardMaps.Where(m => m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardLevel == "Unknown");
                                        //// added new for look up location details.
                                        //if (!string.IsNullOrEmpty(cardlocation))
                                        //    cardMaps = binLocation != null ? cardMaps.Where(m => m.CardLocation.Equals(binLocation.CardLocation, StringComparison.OrdinalIgnoreCase))
                                        //        : null;
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardLocation == "Unknown");

                                        cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase) &&
                                                                       m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase) &&
                                                                       m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));
                                        cardMaps = binLocation != null ? cardMaps.Where(m => m.CardLocation.Equals(binLocation.CardLocation, StringComparison.OrdinalIgnoreCase))
                                                : cardMaps; //27Jun2023

                                        var cardmapId = cardMaps.Select(s => (int)s.CardMapId).FirstOrDefault();
                                        cardmapId = cardmapId == 0 ? 13 : cardmapId;
                                        //16Aug2021
                                        var BinData = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardBrand, s.CardTransType, s.CardLevel, s.BinCountry }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel && m.BinCountry == binlistdata.BinCountry).Select(s => new { s.CardBrand, s.CardTransType, s.CardLevel, s.BinCountry }).FirstOrDefault();
                                        if (cardmapId == 13)
                                        {
                                            //23Nov2021
                                            var BinLists = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.BinCountry, s.Bin }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel && m.BinCountry == binlistdata.BinCountry).Select(s => s.Bin).Distinct().ToArray();
                                            sb.AppendFormat(string.Format("No Card Mapping for this <b> Bin: [{0}] ,  Brand: {1} ,  Type: {2} , Level: {3} , Location: {4} </b>on Client Fees. <br />", BinLists != null ? string.Join(",", BinLists) : null, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, binLocation != null ? binLocation.CardLocation : null));
                                        }
                                        // get card maps from Merchant billing fee Rates table                         
                                        var cardMapIds = dc.LPS_BIL_TF_ClientBillingFeeRates.Where(r => r.MerFeeDetailId == merFeedetailId && r.CardMapId == cardmapId && r.FeeTypeId == billingmethodfeetypeid).Select(r => r.CardMapId).Distinct().ToList();
                                        if (cardMapIds == null || cardMapIds.Count == 0)
                                        {
                                            sb.AppendFormat(string.Format("There is no Client Fee Type setup for this Card Map ( Brand: {0} ,  Type: {1} , Level: {2} , Location: {3} ).<br />", BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, binLocation != null ? binLocation.CardLocation : null));
                                            //throw new Exception(string.Format("No Card Mapping for this  Brand:{0},  Type:{1}, Level:{2}.", binlistdata.CardBrand, binlistdata.CardTransType, binlistdata.CardLevel));
                                            cardmapId = 13;
                                        }
                                        binlistwithLocation.Add(new Tuple<string, string, string, string, int>(BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, binLocation != null ? binLocation.CardLocation : null, cardmapId));
                                    }
                                    foreach (var BinData in binlistwithLocation.Select(s => new { CardBrand = s.Item1, CardTransType = s.Item2, CardLevel = s.Item3, CardLocation = s.Item4, CardMapId = s.Item5 }).Distinct())//14Jul2022
                                    {
                                        var billingFeeTypeIds = dc.LPS_BIL_TF_ClientBillingFeeRates.Where(r => r.CardMapId == BinData.CardMapId && r.MerFeeDetailId == merFeedetailId && r.FeeTypeId == billingmethodfeetypeid).Select(r => r.FeeTypeId).Distinct().ToList();

                                        //This is added for Interchange Fee -- NOTE:  This logic is currently disabled until Mandy confirmed.
                                        if (billingFeeTypeIds.Contains(36) || billingFeeTypeIds.Contains(37))
                                            billingFeeTypeIds.Add(30);

                                        if (billingFeeTypeIds == null || billingFeeTypeIds.Count == 0)
                                            throw new Exception(string.Format("There is no Client Fee Type setup for this  Merchant {0} and cardMap {1}.<br />", billingMerchant.CompanyName, BinData.CardMapId));

                                        foreach (var billingFeeTypeId in billingFeeTypeIds)
                                        {
                                            feeCount += EfDailyStatementTieredFee.CalDailyStatementClientBillingFeeCount_BlendedInterchange(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankConnId, merFeedetailId, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, BinData.CardMapId, BinData.CardLocation, billingFeeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                                        }

                                    }
                                    #endregion
                                    break;
                                case 1: //08Aug2022
                                    #region Step 5(b) Sec 3 : Blended Rate for AU
                                    var BinListAUData = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardTransType = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.BinCountry }).Distinct().ToList();
                                    foreach (var binlistdata in BinListAUData)
                                    {
                                        var binCountry = string.IsNullOrEmpty(binlistdata.BinCountry) ? "AU" : binlistdata.BinCountry;
                                        var cardmapId = 13;
                                        if (binCountry == "AU")
                                        {
                                            //get card MapId
                                            var cardMaps = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardMapId });

                                            //var cardbrand = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower() }).Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardBrand).FirstOrDefault();
                                            //var cardtype = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower() }).Where(m => m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardType).FirstOrDefault();
                                            //var cardlevel = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower() }).Where(m => m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardLevel).FirstOrDefault();

                                            //if (!string.IsNullOrEmpty(cardbrand))
                                            //    cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase));
                                            //else
                                            //    cardMaps = cardMaps.Where(m => m.CardBrand == "Unknown");
                                            //if (!string.IsNullOrEmpty(cardtype))
                                            //    cardMaps = cardMaps.Where(m => m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase));
                                            //else
                                            //    cardMaps = cardMaps.Where(m => m.CardType == "Unknown");
                                            //if (!string.IsNullOrEmpty(cardlevel))
                                            //    cardMaps = cardMaps.Where(m => m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));
                                            //else
                                            //    cardMaps = cardMaps.Where(m => m.CardLevel == "Unknown");

                                            cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase) &&
                                                                       m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase) &&
                                                                       m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));//27Jun2023

                                            cardmapId = cardMaps.Select(s => (int)s.CardMapId).FirstOrDefault();
                                            cardmapId = cardmapId == 0 ? 13 : cardmapId;
                                        }
                                        else //NON AU
                                        {
                                            var brand = binlistdata.CardBrand.Contains("visa") ? "visa" :
                                                        binlistdata.CardBrand.Contains("master") ? "mast" :
                                                        binlistdata.CardBrand.Contains("cirrus") ? "mast" :
                                                        binlistdata.CardBrand.Contains("maestro") ? "mast" :
                                                        binlistdata.CardBrand.Contains("american") ? "amex" : "unknown";

                                            var type = binlistdata.CardTransType.Contains("credit") ? "credit" :
                                                        binlistdata.CardTransType.Contains("debit") ? "debit" : "unknown";
                                            //Donne advised us to consider all the debit international to credit international -17Oct2023
                                            switch (brand)
                                            {
                                                case "visa":
                                                    switch (type)
                                                    {
                                                        case "credit":
                                                            cardmapId = 4;
                                                            break;
                                                        case "debit":
                                                            cardmapId = 4;//68
                                                            break;
                                                        default:
                                                            cardmapId = 13;
                                                            break;
                                                    }
                                                    break;
                                                case "mast":
                                                    switch (type)
                                                    {
                                                        case "credit":
                                                            cardmapId = 10;
                                                            break;
                                                        case "debit":
                                                            cardmapId = 10;//69
                                                            break;
                                                        default:
                                                            cardmapId = 13;
                                                            break;
                                                    }
                                                    break;
                                                case "amex":
                                                    switch (type)
                                                    {
                                                        case "credit":
                                                            cardmapId = 67;
                                                            break;
                                                        case "debit":
                                                            cardmapId = 67;//70
                                                            break;
                                                        default:
                                                            cardmapId = 13;
                                                            break;
                                                    }
                                                    break;
                                                default:
                                                    cardmapId = 13;
                                                    break;
                                            }
                                        }
                                        //16Aug2021
                                        var BinData = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardBrand, s.CardTransType, s.CardLevel, s.BinCountry }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel && m.BinCountry == binlistdata.BinCountry).Select(s => new { s.CardBrand, s.CardTransType, s.CardLevel, s.BinCountry }).FirstOrDefault();
                                        if (cardmapId == 13)
                                        {
                                            //23Nov2021
                                            var BinLists = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.Bin }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel).Select(s => s.Bin).Distinct().ToArray();
                                            sb.AppendFormat(string.Format("No Card Mapping for this <b> Bin: [{0}] ,  Brand: {1} ,  Type: {2} , Level: {3} </b>on Client Fees.<br />", BinLists != null ? string.Join(",", BinLists) : null, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel));
                                        }
                                        // get card maps from Merchant billing fee Rates table                         
                                        var cardMapIds = dc.LPS_BIL_TF_ClientBillingFeeRates.Where(r => r.MerFeeDetailId == merFeedetailId && r.CardMapId == cardmapId && r.FeeTypeId == billingmethodfeetypeid).Select(r => r.CardMapId).Distinct().ToList();
                                        if (cardMapIds == null || cardMapIds.Count == 0)
                                        {
                                            sb.AppendFormat(string.Format("There is no Client Fee Type setup for this Card Map ( Brand: {0} ,  Type: {1} , Level: {2} , Country: {3}, Billing Merchant:{4}).<br />", BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, binCountry == "AU" ? binCountry : "Non AU", billingMerchant.CompanyName));
                                            //throw new Exception(string.Format("No Card Mapping for this  Brand:{0},  Type:{1}, Level:{2}.", binlistdata.CardBrand, binlistdata.CardTransType, binlistdata.CardLevel));
                                            cardmapId = 13;
                                        }

                                        var billingFeeTypeIds = dc.LPS_BIL_TF_ClientBillingFeeRates.Where(r => r.CardMapId == cardmapId && r.MerFeeDetailId == merFeedetailId && r.FeeTypeId == billingmethodfeetypeid).Select(r => r.FeeTypeId).Distinct().ToList();
                                        //This is added for Interchange Fee
                                        if (billingFeeTypeIds.Contains(36) || billingFeeTypeIds.Contains(37))
                                            billingFeeTypeIds.Add(30);

                                        if (billingFeeTypeIds == null || billingFeeTypeIds.Count == 0)
                                            throw new Exception(string.Format("There is no Client Fee Type setup for this  Merchant {0} and cardMap {1}.<br />", billingMerchant.CompanyName, cardmapId));
                                        foreach (var billingFeeTypeId in billingFeeTypeIds)
                                        {
                                            feeCount += EfDailyStatementTieredFee.CalDailyStatementClientBillingFeeCount(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankConnId, merFeedetailId, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, BinData.BinCountry, cardmapId, billingFeeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                                        }

                                    }
                                    #endregion
                                    break;
                            }
                        }
                        #endregion

                        #endregion

                        #region Step 6:Bank Fee

                        //get Feetype from Bank Fee details
                        var bankFeedetailId = dc.LPS_BIL_MerchantFeeDetails.Where(r => r.BillingMerchantId == billingMerchant.BillingMerchantId && r.BankConnId == bankConnId && r.CurrencyCode == currency && r.IsActive == true && r.FeeLevelId == 2).OrderByDescending(s => s.MerFeeDetailId).Select(r => r.MerFeeDetailId).FirstOrDefault();
                        var midMgmtId = dc.LPS_BIL_MapMerchantWithBankRates.Where(m => m.MerFeeDetailId == bankFeedetailId).OrderByDescending(m => m.MapMerBankId).Select(s => s.MidMgmtId).FirstOrDefault();
                        var bankfeeTypeIds = dc.LPS_BIL_BankFeeRates.Where(r => r.MidMgmtId == midMgmtId).Select(r => r.FeeTypeId).Distinct().ToList();
                        if (bankfeeTypeIds == null || bankfeeTypeIds.Count == 0)
                            if (bankConnId == 88) //issue on 05Sep2024
                            {
                                sb.AppendFormat(string.Format("There is no Bank FeeType setup for the particular Merchant {0}.<br />", billingMerchant.CompanyName));
                                continue;
                            }
                            else
                                throw new Exception(string.Format("There is no Bank FeeType setup for the particular Merchant {0}.<br />", billingMerchant.CompanyName));

                        #region  Step 6(a): Insert into BanktFeeRates table with actual calculations.

                        foreach (var bankfeeTypeId in bankfeeTypeIds)
                        {
                            feeCount += EfDailyStatementTieredFee.CalDailyStatementBankFeeCount(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankId, bankConnId, midMgmtId, bankfeeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                        }
                        #endregion

                        #region  Step 6(b): Insert into Merchant Billing Fee Rates table with actual calculations.

                        // Get list of billing fee types for this billing merchant and based on this retrive the billing method id to check whether its Blended or Blended interchange
                        var billingMethodBankFeeTypeIds = dc.LPS_BIL_BankBillingFeeRates.Where(r => r.MidMgmtId == midMgmtId).Select(r => r.FeeTypeId).Distinct().ToList();
                        // To check empty or null records
                        if (billingMethodBankFeeTypeIds == null || billingMethodBankFeeTypeIds.Count == 0)
                            throw new Exception(string.Format("There is no Bank billing feetype setup for this Merchant {0}.<br />", billingMerchant.CompanyName));

                        // Loop through billing fee types to find billing method id.
                        foreach (var billingmethodfeetypeid in billingMethodBankFeeTypeIds)
                        {
                            // Retrieve the Fee Group of the fee type
                            var feeGroupId = dc.LPS_BIL_Cfg_FeeTypes.Where(t => t.FeeTypeId == billingmethodfeetypeid).Select(t => t.FeeGroupId).FirstOrDefault();

                            // Retrieve the actual billing Method id for proceed further
                            var billingMethodId = dc.LPS_BIL_Cfg_BillingFeeTypes.Where(t => t.FeeGroupId == feeGroupId).Select(t => t.BillingFeeTypeId).FirstOrDefault();
                            switch (billingMethodId)
                            {
                                //usp_BIL_GetCardDetails replaced usp_BIL_GetCardDetailsfromMultipleBankConn 14Feb2023
                                case 4:
                                    #region step 6(a) Sec 1: Blended Rate
                                    // get card maps from Merchant billing fee Rates table         
                                    var BinListDataforBank = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardTransType = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower() }).Distinct().ToList();
                                    foreach (var binlistdata in BinListDataforBank)
                                    {
                                        //get card MapId
                                        var cardMaps = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardMapId });

                                        //var cardbrand = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower() }).Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardBrand).FirstOrDefault();
                                        //var cardtype = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower() }).Where(m => m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardType).FirstOrDefault();
                                        //var cardlevel = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower() }).Where(m => m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardLevel).FirstOrDefault();

                                        //if (!string.IsNullOrEmpty(cardbrand))
                                        //    cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase));
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardBrand == "Unknown");
                                        //if (!string.IsNullOrEmpty(cardtype))
                                        //    cardMaps = cardMaps.Where(m => m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase));
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardType == "Unknown");
                                        //if (!string.IsNullOrEmpty(cardlevel))
                                        //    cardMaps = cardMaps.Where(m => m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardLevel == "Unknown");

                                        cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase) &&
                                                                       m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase) &&
                                                                       m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));//27Jun2023

                                        var cardmapId = cardMaps.Select(s => (int)s.CardMapId).FirstOrDefault();
                                        cardmapId = cardmapId == 0 ? 13 : cardmapId;
                                        //16Aug2021
                                        var BinData = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardBrand, s.CardTransType, s.CardLevel }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel).Select(s => new { s.CardBrand, s.CardTransType, s.CardLevel }).FirstOrDefault();
                                        if (cardmapId == 13)
                                        {
                                            //23Nov2021
                                            var BinLists = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.Bin }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel).Select(s => s.Bin).Distinct().ToArray();
                                            //sb.AppendFormat(string.Format("No Card Mapping for this <b> Bin: [{0}] ,  Brand: {1} ,  Type: {2} , Level: {3} </b>on Bank Fees.<br />", BinLists != null ? string.Join(",", BinLists) : null, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel));
                                        }
                                        // get card maps from Merchant billing fee Rates table                           
                                        var bankcardMapIds = dc.LPS_BIL_BankBillingFeeRates.Where(r => r.MidMgmtId == midMgmtId && r.CardMapId == cardmapId && r.FeeTypeId == billingmethodfeetypeid).Distinct().ToList();
                                        if (bankcardMapIds == null || bankcardMapIds.Count == 0)
                                        {
                                            sb.AppendFormat(string.Format("There is no Bank Fee Type setup for this Card Map ( Brand: {0} ,  Type: {1} , Level: {2}, Billing Merchant:{3} ).<br />", BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, billingMerchant.CompanyName));
                                            //throw new Exception(string.Format("No Card Mapping for this  Brand:{0},  Type:{1},  Level:{2}.", binlistdata.CardBrand, binlistdata.CardTransType, binlistdata.CardLevel));
                                            cardmapId = 13;
                                        }
                                        foreach (var bankcardMapId in bankcardMapIds)
                                        {
                                            var billingFeeTypeIds = dc.LPS_BIL_BankBillingFeeRates.Where(r => r.CardMapId == cardmapId && r.MidMgmtId == midMgmtId && r.FeeTypeId == billingmethodfeetypeid).Select(r => r.FeeTypeId).Distinct().ToList();

                                            //This is added for Interchange Fee
                                            if (billingFeeTypeIds.Contains(36) || billingFeeTypeIds.Contains(37))
                                                billingFeeTypeIds.Add(30);

                                            if (billingFeeTypeIds == null || billingFeeTypeIds.Count == 0)
                                                throw new Exception(string.Format("There is no Bank Fee Type setup for this  Merchant {0} and cardMap {1}.<br />", billingMerchant.CompanyName, bankcardMapId));
                                            foreach (var billingFeeTypeId in billingFeeTypeIds)
                                            {
                                                feeCount += EfDailyStatementTieredFee.CalDailyStatementBankBillingFeeCount(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankConnId, midMgmtId, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, cardmapId, billingFeeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                                            }
                                        }

                                    }
                                    break;
                                #endregion
                                case 3:
                                    #region step 6(b) Sec 1: Blended Interchange Rate
                                    var binlistwithLocation = new List<Tuple<string, string, string, string, int>>();//14Jul2022
                                                                                                                     // get card maps from Merchant billing fee Rates table         
                                    var BinListIntraDataforBank = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardTransType = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.BinCountry }).Distinct().ToList();
                                    foreach (var binlistdata in BinListIntraDataforBank)
                                    {
                                        var binCountry = string.IsNullOrEmpty(binlistdata.BinCountry) ? "GB" : binlistdata.BinCountry; //30Aug2022

                                        //Added
                                        var binLocation = dc.usp_BIL_InterchangMappingDetails(billingMerchant.MerchantId, billingMerchant.BillingMerchantId, binCountry).FirstOrDefault();

                                        //get card MapId
                                        var cardMaps = dc.LPS_BIL_Cfg_CardLevelIntraGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardLocation, s.CardMapId });

                                        //var cardbrand = dc.LPS_BIL_Cfg_CardLevelIntraGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower() }).Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardBrand).FirstOrDefault();
                                        //var cardtype = dc.LPS_BIL_Cfg_CardLevelIntraGrouping.Select(s => new { CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower() }).Where(m => m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardType).FirstOrDefault();
                                        //var cardlevel = dc.LPS_BIL_Cfg_CardLevelIntraGrouping.Select(s => new { CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower() }).Where(m => m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardLevel).FirstOrDefault();
                                        //var cardlocation = binLocation != null ? dc.LPS_BIL_Cfg_CardLevelIntraGrouping.Where(m => m.CardLocation.Equals(binLocation.CardLocation, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardLocation).FirstOrDefault()
                                        //                                         : null;

                                        //if (!string.IsNullOrEmpty(cardbrand))
                                        //    cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase));
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardBrand == "Unknown");
                                        //if (!string.IsNullOrEmpty(cardtype))
                                        //    cardMaps = cardMaps.Where(m => m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase));
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardType == "Unknown");
                                        //if (!string.IsNullOrEmpty(cardlevel))
                                        //    cardMaps = cardMaps.Where(m => m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardLevel == "Unknown");
                                        //// added new for look up location details.
                                        //if (!string.IsNullOrEmpty(cardlocation))
                                        //    cardMaps = binLocation != null ? cardMaps.Where(m => m.CardLocation.Equals(binLocation.CardLocation, StringComparison.OrdinalIgnoreCase))
                                        //        : null;
                                        //else
                                        //    cardMaps = cardMaps.Where(m => m.CardLocation == "Unknown");

                                        cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase) &&
                                                                       m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase) &&
                                                                       m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));
                                        cardMaps = binLocation != null ? cardMaps.Where(m => m.CardLocation.Equals(binLocation.CardLocation, StringComparison.OrdinalIgnoreCase))
                                               : cardMaps;//27Jun2023

                                        var cardmapId = cardMaps.Select(s => (int)s.CardMapId).FirstOrDefault();
                                        cardmapId = cardmapId == 0 ? 13 : cardmapId;
                                        //16Aug2021
                                        var BinData = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardBrand, s.CardTransType, s.CardLevel, s.BinCountry }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel && m.BinCountry == binlistdata.BinCountry).Select(s => new { s.CardBrand, s.CardTransType, s.CardLevel, s.BinCountry }).FirstOrDefault();
                                        if (cardmapId == 13)
                                        {
                                            //23Nov2021
                                            var BinLists = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.BinCountry, s.Bin }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel && m.BinCountry == binlistdata.BinCountry).Select(s => s.Bin).Distinct().ToArray();
                                            // sb.AppendFormat(string.Format("No Card Mapping for this <b> Bin: [{0}] ,  Brand: {1} ,  Type: {2} , Level: {3} , Location: {4} </b>on Bank Fees. <br />", BinLists != null ? string.Join(",", BinLists) : null, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, binLocation != null ? binLocation.CardLocation : null));
                                        }
                                        // get card maps from Merchant billing fee Rates table                           
                                        var bankcardMapIds = dc.LPS_BIL_BankBillingFeeRates.Where(r => r.MidMgmtId == midMgmtId && r.CardMapId == cardmapId && r.FeeTypeId == billingmethodfeetypeid).Distinct().ToList();
                                        if (bankcardMapIds == null || bankcardMapIds.Count == 0)
                                        {
                                            sb.AppendFormat(string.Format("There is no Bank Fee Type setup for this Card Map ( Brand: {0} ,  Type: {1} , Level: {2} , Location: {3} ).<br />", BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, binLocation != null ? binLocation.CardLocation : null));
                                            //throw new Exception(string.Format("No Card Mapping for this  Brand:{0},  Type:{1},  Level:{2}.", binlistdata.CardBrand, binlistdata.CardTransType, binlistdata.CardLevel));
                                            cardmapId = 13;
                                        }
                                        binlistwithLocation.Add(new Tuple<string, string, string, string, int>(BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, binLocation != null ? binLocation.CardLocation : null, cardmapId));
                                    }
                                    foreach (var BinData in binlistwithLocation.Select(s => new { CardBrand = s.Item1, CardTransType = s.Item2, CardLevel = s.Item3, CardLocation = s.Item4, CardMapId = s.Item5 }).Distinct())//14Jul2022
                                    {
                                        var billingFeeTypeIds = dc.LPS_BIL_BankBillingFeeRates.Where(r => r.CardMapId == BinData.CardMapId && r.MidMgmtId == midMgmtId && r.FeeTypeId == billingmethodfeetypeid).Select(r => r.FeeTypeId).Distinct().ToList();

                                        // NOTE: This logic is currently disabled until Mandy confirmed.
                                        //This is added for Interchange Fee
                                        if (billingFeeTypeIds.Contains(36) || billingFeeTypeIds.Contains(37))
                                            billingFeeTypeIds.Add(30);

                                        if (billingFeeTypeIds == null || billingFeeTypeIds.Count == 0)
                                            throw new Exception(string.Format("There is no Bank Fee Type setup for this  Merchant {0} and cardMap {1}.<br />", billingMerchant.CompanyName, BinData.CardMapId));

                                        foreach (var billingFeeTypeId in billingFeeTypeIds)
                                        {
                                            feeCount += EfDailyStatementTieredFee.CalDailyStatementBankBillingFeeCount_BlendedInterchange(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankConnId, midMgmtId, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, BinData.CardMapId, BinData.CardLocation, billingFeeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                                        }
                                    }
                                    break;
                                #endregion
                                case 1: //08Aug2022
                                    #region Step 6(b) Sec 3 : Blended Rate for AU
                                    // get card maps from Merchant billing fee Rates table         
                                    var BinListAUDataforBank = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardTransType = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.BinCountry }).Distinct().ToList();
                                    foreach (var binlistdata in BinListAUDataforBank)
                                    {
                                        var binCountry = string.IsNullOrEmpty(binlistdata.BinCountry) ? "AU" : binlistdata.BinCountry;
                                        var cardmapId = 13;
                                        if (binCountry == "AU")
                                        {
                                            //get card MapId
                                            var cardMaps = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardMapId });

                                            //var cardbrand = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower() }).Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardBrand).FirstOrDefault();
                                            //var cardtype = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower() }).Where(m => m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardType).FirstOrDefault();
                                            //var cardlevel = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower() }).Where(m => m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase)).Select(s => s.CardLevel).FirstOrDefault();

                                            //if (!string.IsNullOrEmpty(cardbrand))
                                            //    cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase));
                                            //else
                                            //    cardMaps = cardMaps.Where(m => m.CardBrand == "Unknown");
                                            //if (!string.IsNullOrEmpty(cardtype))
                                            //    cardMaps = cardMaps.Where(m => m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase));
                                            //else
                                            //    cardMaps = cardMaps.Where(m => m.CardType == "Unknown");
                                            //if (!string.IsNullOrEmpty(cardlevel))
                                            //    cardMaps = cardMaps.Where(m => m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));
                                            //else
                                            //    cardMaps = cardMaps.Where(m => m.CardLevel == "Unknown");

                                            cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase) &&
                                                                       m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase) &&
                                                                       m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));//27Jun2023

                                            cardmapId = cardMaps.Select(s => (int)s.CardMapId).FirstOrDefault();
                                            cardmapId = cardmapId == 0 ? 13 : cardmapId;
                                        }
                                        else //NON AU
                                        {
                                            var brand = binlistdata.CardBrand.Contains("visa") ? "visa" :
                                                        binlistdata.CardBrand.Contains("master") ? "mast" :
                                                        binlistdata.CardBrand.Contains("american") ? "amex" : "unknown";

                                            var type = binlistdata.CardTransType.Contains("credit") ? "credit" :
                                                        binlistdata.CardTransType.Contains("debit") ? "debit" : "unknown";
                                            //Donne advised us to consider all the debit international to credit international -17Oct2023
                                            switch (brand)
                                            {
                                                case "visa":
                                                    switch (type)
                                                    {
                                                        case "credit":
                                                            cardmapId = 4;
                                                            break;
                                                        case "debit":
                                                            cardmapId = 4;//68
                                                            break;
                                                        default:
                                                            cardmapId = 13;
                                                            break;
                                                    }
                                                    break;
                                                case "mast":
                                                    switch (type)
                                                    {
                                                        case "credit":
                                                            cardmapId = 10;
                                                            break;
                                                        case "debit":
                                                            cardmapId = 10;//69
                                                            break;
                                                        default:
                                                            cardmapId = 13;
                                                            break;
                                                    }
                                                    break;
                                                case "amex":
                                                    switch (type)
                                                    {
                                                        case "credit":
                                                            cardmapId = 67;
                                                            break;
                                                        case "debit":
                                                            cardmapId = 67;//70
                                                            break;
                                                        default:
                                                            cardmapId = 13;
                                                            break;
                                                    }
                                                    break;
                                                default:
                                                    cardmapId = 13;
                                                    break;
                                            }
                                        }
                                        //16Aug2021
                                        var BinData = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardBrand, s.CardTransType, s.CardLevel, s.BinCountry }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel && m.BinCountry == binlistdata.BinCountry).Select(s => new { s.CardBrand, s.CardTransType, s.CardLevel, s.BinCountry }).FirstOrDefault();
                                        if (cardmapId == 13)
                                        {
                                            //23Nov2021
                                            var BinLists = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.Bin }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel).Select(s => s.Bin).Distinct().ToArray();
                                            //sb.AppendFormat(string.Format("No Card Mapping for this <b> Bin: [{0}] ,  Brand: {1} ,  Type: {2} , Level: {3} </b>on Bank Fees.<br />", BinLists != null ? string.Join(",", BinLists) : null, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel));
                                        }
                                        // get card maps from Merchant billing fee Rates table                           
                                        var bankcardMapIds = dc.LPS_BIL_BankBillingFeeRates.Where(r => r.MidMgmtId == midMgmtId && r.CardMapId == cardmapId && r.FeeTypeId == billingmethodfeetypeid).Distinct().ToList();
                                        if (bankcardMapIds == null || bankcardMapIds.Count == 0)
                                        {
                                            sb.AppendFormat(string.Format("There is no Bank Fee Type setup for this Card Map ( Brand: {0} ,  Type: {1} , Level: {2} , Country: {3} , Billing Merchant: {4}).<br />", BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, binCountry == "AU" ? binCountry : "Non AU", billingMerchant.CompanyName));
                                            //throw new Exception(string.Format("No Card Mapping for this  Brand:{0},  Type:{1},  Level:{2}.", binlistdata.CardBrand, binlistdata.CardTransType, binlistdata.CardLevel));
                                            cardmapId = 13;
                                        }
                                        foreach (var bankcardMapId in bankcardMapIds)
                                        {
                                            var billingFeeTypeIds = dc.LPS_BIL_BankBillingFeeRates.Where(r => r.CardMapId == cardmapId && r.MidMgmtId == midMgmtId && r.FeeTypeId == billingmethodfeetypeid).Select(r => r.FeeTypeId).Distinct().ToList();

                                            //This is added for Interchange Fee
                                            if (billingFeeTypeIds.Contains(36) || billingFeeTypeIds.Contains(37))
                                                billingFeeTypeIds.Add(30);

                                            if (billingFeeTypeIds == null || billingFeeTypeIds.Count == 0)
                                                throw new Exception(string.Format("There is no Bank Fee Type setup for this  Merchant {0} and cardMap {1}.<br />", billingMerchant.CompanyName, bankcardMapId));
                                            foreach (var billingFeeTypeId in billingFeeTypeIds)
                                            {
                                                feeCount += EfDailyStatementTieredFee.CalDailyStatementBankBillingFeeCount(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankConnId, midMgmtId, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, BinData.BinCountry, cardmapId, billingFeeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                                            }
                                        }

                                    }
                                    #endregion
                                    break;
                            }


                            //// get card maps from Merchant billing fee Rates table                           
                            //var bankcardMapIds = dc.LPS_BIL_BankBillingFeeRates.Where(r => r.MidMgmtId == midMgmtId).Select(r => r.CardMapId).Distinct().ToList();
                            //if (bankcardMapIds == null)
                            //    throw new Exception(string.Format("There is no Card configured for this Merchant {0}.", billingMerchant.CompanyName));

                            //foreach (var bankcardMapId in bankcardMapIds)
                            //{
                            //    var billingFeeTypeIds = dc.LPS_BIL_BankBillingFeeRates.Where(r => r.CardMapId == bankcardMapId && r.MidMgmtId == midMgmtId).Select(r => r.FeeTypeId).Distinct().ToList();

                            //    //This is added for Interchange Fee
                            //    if (billingFeeTypeIds.Contains(36) || billingFeeTypeIds.Contains(37))
                            //        billingFeeTypeIds.Add(30);

                            //    if (billingFeeTypeIds == null)
                            //        throw new Exception(string.Format("There is no Bank Fee Type setup for this  Mercahnt {0} and cardMap {1},", billingMerchant.CompanyName, bankcardMapId));

                            //    foreach (var billingFeeTypeId in billingFeeTypeIds)
                            //    {
                            //        feeCount += EfDailyStatementTieredFee.CalDailyStatementBankBillingFeeCount(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankConnId, midMgmtId, bankcardMapId, billingFeeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                            //    }
                            //}



                        }
                        #endregion

                        #endregion

                        #region Partner Cost of process Fee      
                        if (dc.usp_BIL_GetPartnerCommisionRates(billingMerchant.BillingMerchantId, bankConnId, currency).Select(s => s.Type == "COP Fees").Any())
                        {
                            var costFeedetailId = dc.LPS_BIL_MerchantFeeDetails.Where(r => r.BillingMerchantId == billingMerchant.BillingMerchantId && r.BankConnId == bankConnId && r.CurrencyCode == currency && r.IsActive == true && r.FeeLevelId == 3).OrderByDescending(s => s.MerFeeDetailId).Select(r => r.MerFeeDetailId).FirstOrDefault();
                            var cost_midMgmtId = dc.LPS_BIL_MapMerchantWithCOPRates.Where(m => m.MerFeeDetailId == costFeedetailId).OrderByDescending(m => m.MapMerCopId).Select(s => s.MidMgmtId).FirstOrDefault();
                            var costfeeTypeIds = dc.LPS_BIL_CostProcFeeRates.Where(r => r.MidMgmtId == cost_midMgmtId).Select(r => r.FeeTypeId).Distinct().ToList();
                            if (costfeeTypeIds == null || costfeeTypeIds.Count == 0)
                                throw new Exception(string.Format("There is no Cost of FeeType setup for the particular Merchant {0}.<br />", billingMerchant.CompanyName));

                            #region  Step 7(a): Insert into CostProcFeeRates table with actual calculations.

                            foreach (var bankfeeTypeId in costfeeTypeIds)
                            {
                                feeCount += EfDailyStatementTieredFee.CalDailyStatementCostofProcessFeeCount(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankId, bankConnId, cost_midMgmtId, bankfeeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                            }
                            #endregion

                            #region  Step 7(b): Insert into Merchant Billing Fee Rates table with actual calculations.

                            // Get list of billing fee types for this billing merchant and based on this retrive the billing method id to check whether its Blended or Blended interchange
                            var billingMethodCostFeeTypeIds = dc.LPS_BIL_CostProcBillingFeeRates.Where(r => r.MidMgmtId == cost_midMgmtId).Select(r => r.FeeTypeId).Distinct().ToList();
                            // To check empty or null records
                            if (billingMethodCostFeeTypeIds == null || billingMethodCostFeeTypeIds.Count == 0)
                                throw new Exception(string.Format("There is no Cost of billing feetype setup for this Merchant {0}.<br />", billingMerchant.CompanyName));

                            // Loop through billing fee types to find billing method id.
                            foreach (var billingmethodfeetypeid in billingMethodCostFeeTypeIds)
                            {
                                // Retrieve the Fee Group of the fee type
                                var feeGroupId = dc.LPS_BIL_Cfg_FeeTypes.Where(t => t.FeeTypeId == billingmethodfeetypeid).Select(t => t.FeeGroupId).FirstOrDefault();

                                // Retrieve the actual billing Method id for proceed further
                                var billingMethodId = dc.LPS_BIL_Cfg_BillingFeeTypes.Where(t => t.FeeGroupId == feeGroupId).Select(t => t.BillingFeeTypeId).FirstOrDefault();
                                switch (billingMethodId)
                                {
                                    //usp_BIL_GetCardDetails replaced usp_BIL_GetCardDetailsfromMultipleBankConn 14Feb2023
                                    case 4:
                                        #region step 7(b) Sec 1: Blended Rate
                                        // get card maps from Merchant billing fee Rates table         
                                        var BinListDataforBank = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardTransType = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower() }).Distinct().ToList();
                                        foreach (var binlistdata in BinListDataforBank)
                                        {
                                            //get card MapId
                                            var cardMaps = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardMapId });

                                            cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase) &&
                                                                           m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase) &&
                                                                           m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));//27Jun2023

                                            var cardmapId = cardMaps.Select(s => (int)s.CardMapId).FirstOrDefault();
                                            cardmapId = cardmapId == 0 ? 13 : cardmapId;
                                            //16Aug2021
                                            var BinData = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardBrand, s.CardTransType, s.CardLevel }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel).Select(s => new { s.CardBrand, s.CardTransType, s.CardLevel }).FirstOrDefault();
                                            if (cardmapId == 13)
                                            {
                                                //23Nov2021
                                                var BinLists = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.Bin }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel).Select(s => s.Bin).Distinct().ToArray();
                                                //sb.AppendFormat(string.Format("No Card Mapping for this <b> Bin: [{0}] ,  Brand: {1} ,  Type: {2} , Level: {3} </b>on Bank Fees.<br />", BinLists != null ? string.Join(",", BinLists) : null, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel));
                                            }
                                            // get card maps from Merchant billing fee Rates table                           
                                            var bankcardMapIds = dc.LPS_BIL_CostProcBillingFeeRates.Where(r => r.MidMgmtId == cost_midMgmtId && r.CardMapId == cardmapId && r.FeeTypeId == billingmethodfeetypeid).Distinct().ToList();
                                            if (bankcardMapIds == null || bankcardMapIds.Count == 0)
                                            {
                                                sb.AppendFormat(string.Format("There is no Cost of process Fee Type setup for this Card Map ( Brand: {0} ,  Type: {1} , Level: {2}, Billing Merchant:{3} ).<br />", BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, billingMerchant.CompanyName));
                                                //throw new Exception(string.Format("No Card Mapping for this  Brand:{0},  Type:{1},  Level:{2}.", binlistdata.CardBrand, binlistdata.CardTransType, binlistdata.CardLevel));
                                                cardmapId = 13;
                                            }
                                            foreach (var bankcardMapId in bankcardMapIds)
                                            {
                                                var billingFeeTypeIds = dc.LPS_BIL_CostProcBillingFeeRates.Where(r => r.CardMapId == cardmapId && r.MidMgmtId == cost_midMgmtId && r.FeeTypeId == billingmethodfeetypeid).Select(r => r.FeeTypeId).Distinct().ToList();

                                                //This is added for Interchange Fee
                                                if (billingFeeTypeIds.Contains(36) || billingFeeTypeIds.Contains(37))
                                                    billingFeeTypeIds.Add(30);

                                                if (billingFeeTypeIds == null || billingFeeTypeIds.Count == 0)
                                                    throw new Exception(string.Format("There is no Bank Fee Type setup for this  Merchant {0} and cardMap {1}.<br />", billingMerchant.CompanyName, bankcardMapId));
                                                foreach (var billingFeeTypeId in billingFeeTypeIds)
                                                {
                                                    feeCount += EfDailyStatementTieredFee.CalDailyStatementBankBillingFeeCount(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankConnId, midMgmtId, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, cardmapId, billingFeeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                                                }
                                            }

                                        }
                                        break;
                                    #endregion
                                    case 3:
                                        #region step 7(b) Sec 1: Blended Interchange Rate
                                        var binlistwithLocation = new List<Tuple<string, string, string, string, int>>();//14Jul2022
                                                                                                                         // get card maps from Merchant billing fee Rates table         
                                        var BinListIntraDataforBank = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardTransType = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.BinCountry }).Distinct().ToList();
                                        foreach (var binlistdata in BinListIntraDataforBank)
                                        {
                                            var binCountry = string.IsNullOrEmpty(binlistdata.BinCountry) ? "GB" : binlistdata.BinCountry; //30Aug2022

                                            //Added
                                            var binLocation = dc.usp_BIL_InterchangMappingDetails(billingMerchant.MerchantId, billingMerchant.BillingMerchantId, binCountry).FirstOrDefault();

                                            //get card MapId
                                            var cardMaps = dc.LPS_BIL_Cfg_CardLevelIntraGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardLocation, s.CardMapId });

                                            cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase) &&
                                                                           m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase) &&
                                                                           m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));
                                            cardMaps = binLocation != null ? cardMaps.Where(m => m.CardLocation.Equals(binLocation.CardLocation, StringComparison.OrdinalIgnoreCase))
                                                   : cardMaps;//27Jun2023

                                            var cardmapId = cardMaps.Select(s => (int)s.CardMapId).FirstOrDefault();
                                            cardmapId = cardmapId == 0 ? 13 : cardmapId;
                                            //16Aug2021
                                            var BinData = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardBrand, s.CardTransType, s.CardLevel, s.BinCountry }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel && m.BinCountry == binlistdata.BinCountry).Select(s => new { s.CardBrand, s.CardTransType, s.CardLevel, s.BinCountry }).FirstOrDefault();
                                            if (cardmapId == 13)
                                            {
                                                //23Nov2021
                                                var BinLists = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.BinCountry, s.Bin }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel && m.BinCountry == binlistdata.BinCountry).Select(s => s.Bin).Distinct().ToArray();
                                                // sb.AppendFormat(string.Format("No Card Mapping for this <b> Bin: [{0}] ,  Brand: {1} ,  Type: {2} , Level: {3} , Location: {4} </b>on Bank Fees. <br />", BinLists != null ? string.Join(",", BinLists) : null, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, binLocation != null ? binLocation.CardLocation : null));
                                            }
                                            // get card maps from Merchant billing fee Rates table                           
                                            var bankcardMapIds = dc.LPS_BIL_CostProcBillingFeeRates.Where(r => r.MidMgmtId == midMgmtId && r.CardMapId == cardmapId && r.FeeTypeId == billingmethodfeetypeid).Distinct().ToList();
                                            if (bankcardMapIds == null || bankcardMapIds.Count == 0)
                                            {
                                                sb.AppendFormat(string.Format("There is no Bank Fee Type setup for this Card Map ( Brand: {0} ,  Type: {1} , Level: {2} , Location: {3} ).<br />", BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, binLocation != null ? binLocation.CardLocation : null));
                                                //throw new Exception(string.Format("No Card Mapping for this  Brand:{0},  Type:{1},  Level:{2}.", binlistdata.CardBrand, binlistdata.CardTransType, binlistdata.CardLevel));
                                                cardmapId = 13;
                                            }
                                            binlistwithLocation.Add(new Tuple<string, string, string, string, int>(BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, binLocation.CardLocation, cardmapId));
                                        }
                                        foreach (var BinData in binlistwithLocation.Select(s => new { CardBrand = s.Item1, CardTransType = s.Item2, CardLevel = s.Item3, CardLocation = s.Item4, CardMapId = s.Item5 }).Distinct())//14Jul2022
                                        {
                                            var billingFeeTypeIds = dc.LPS_BIL_CostProcBillingFeeRates.Where(r => r.CardMapId == BinData.CardMapId && r.MidMgmtId == cost_midMgmtId && r.FeeTypeId == billingmethodfeetypeid).Select(r => r.FeeTypeId).Distinct().ToList();

                                            // NOTE: This logic is currently disabled until Mandy confirmed.
                                            //This is added for Interchange Fee
                                            if (billingFeeTypeIds.Contains(36) || billingFeeTypeIds.Contains(37))
                                                billingFeeTypeIds.Add(30);

                                            if (billingFeeTypeIds == null || billingFeeTypeIds.Count == 0)
                                                throw new Exception(string.Format("There is no Bank Fee Type setup for this  Merchant {0} and cardMap {1}.<br />", billingMerchant.CompanyName, BinData.CardMapId));

                                            foreach (var billingFeeTypeId in billingFeeTypeIds)
                                            {
                                                feeCount += EfDailyStatementTieredFee.CalDailyStatementCostofProcessBillingFeeCount_BlendedInterchange(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankConnId, cost_midMgmtId, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, BinData.CardMapId, BinData.CardLocation, billingFeeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                                            }
                                        }
                                        break;
                                    #endregion
                                    case 1: //08Aug2022
                                        #region Step 7(b) Sec 3 : Blended Rate for AU
                                        // get card maps from Merchant billing fee Rates table         
                                        var BinListAUDataforBank = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardTransType = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.BinCountry }).Distinct().ToList();
                                        foreach (var binlistdata in BinListAUDataforBank)
                                        {
                                            var binCountry = string.IsNullOrEmpty(binlistdata.BinCountry) ? "AU" : binlistdata.BinCountry;
                                            var cardmapId = 13;
                                            if (binCountry == "AU")
                                            {
                                                //get card MapId
                                                var cardMaps = dc.LPS_BIL_Cfg_CardLevelGrouping.Select(s => new { CardBrand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), CardType = s.CardType == null ? s.CardType : s.CardType.Trim().ToLower(), CardLevel = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardMapId });

                                                cardMaps = cardMaps.Where(m => m.CardBrand.Equals(binlistdata.CardBrand, StringComparison.OrdinalIgnoreCase) &&
                                                                           m.CardType.Equals(binlistdata.CardTransType, StringComparison.OrdinalIgnoreCase) &&
                                                                           m.CardLevel.Equals(binlistdata.CardLevel, StringComparison.OrdinalIgnoreCase));//27Jun2023

                                                cardmapId = cardMaps.Select(s => (int)s.CardMapId).FirstOrDefault();
                                                cardmapId = cardmapId == 0 ? 13 : cardmapId;
                                            }
                                            else //NON AU
                                            {
                                                var brand = binlistdata.CardBrand.Contains("visa") ? "visa" :
                                                            binlistdata.CardBrand.Contains("master") ? "mast" :
                                                            binlistdata.CardBrand.Contains("american") ? "amex" : "unknown";

                                                var type = binlistdata.CardTransType.Contains("credit") ? "credit" :
                                                            binlistdata.CardTransType.Contains("debit") ? "debit" : "unknown";
                                                //Donne advised us to consider all the debit international to credit international -17Oct2023
                                                switch (brand)
                                                {
                                                    case "visa":
                                                        switch (type)
                                                        {
                                                            case "credit":
                                                                cardmapId = 4;
                                                                break;
                                                            case "debit":
                                                                cardmapId = 4;//68
                                                                break;
                                                            default:
                                                                cardmapId = 13;
                                                                break;
                                                        }
                                                        break;
                                                    case "mast":
                                                        switch (type)
                                                        {
                                                            case "credit":
                                                                cardmapId = 10;
                                                                break;
                                                            case "debit":
                                                                cardmapId = 10;//69
                                                                break;
                                                            default:
                                                                cardmapId = 13;
                                                                break;
                                                        }
                                                        break;
                                                    case "amex":
                                                        switch (type)
                                                        {
                                                            case "credit":
                                                                cardmapId = 67;
                                                                break;
                                                            case "debit":
                                                                cardmapId = 67;//70
                                                                break;
                                                            default:
                                                                cardmapId = 13;
                                                                break;
                                                        }
                                                        break;
                                                    default:
                                                        cardmapId = 13;
                                                        break;
                                                }
                                            }
                                            //16Aug2021
                                            var BinData = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.CardBrand, s.CardTransType, s.CardLevel, s.BinCountry }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel && m.BinCountry == binlistdata.BinCountry).Select(s => new { s.CardBrand, s.CardTransType, s.CardLevel, s.BinCountry }).FirstOrDefault();
                                            if (cardmapId == 13)
                                            {
                                                //23Nov2021
                                                var BinLists = dtRealTime.usp_BIL_GetCardDetailsfromMultipleBankConn(statementStartDate, site.Merchant_User_Id, site.BankConnId, site.CurrencyCode, timeOffset, statementEndDate).Select(s => new { brand = s.CardBrand == null ? s.CardBrand : s.CardBrand.Trim().ToLower(), type = s.CardTransType == null ? s.CardTransType : s.CardTransType.Trim().ToLower(), level = s.CardLevel == null ? s.CardLevel : s.CardLevel.Trim().ToLower(), s.Bin }).Where(m => m.brand == binlistdata.CardBrand && m.type == binlistdata.CardTransType && m.level == binlistdata.CardLevel).Select(s => s.Bin).Distinct().ToArray();
                                                //sb.AppendFormat(string.Format("No Card Mapping for this <b> Bin: [{0}] ,  Brand: {1} ,  Type: {2} , Level: {3} </b>on Bank Fees.<br />", BinLists != null ? string.Join(",", BinLists) : null, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel));
                                            }
                                            // get card maps from Merchant billing fee Rates table                           
                                            var bankcardMapIds = dc.LPS_BIL_CostProcBillingFeeRates.Where(r => r.MidMgmtId == cost_midMgmtId && r.CardMapId == cardmapId && r.FeeTypeId == billingmethodfeetypeid).Distinct().ToList();
                                            if (bankcardMapIds == null || bankcardMapIds.Count == 0)
                                            {
                                                sb.AppendFormat(string.Format("There is no Cost of process Fee Type setup for this Card Map ( Brand: {0} ,  Type: {1} , Level: {2} , Country: {3} , Billing Merchant: {4}).<br />", BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, binCountry == "AU" ? binCountry : "Non AU", billingMerchant.CompanyName));
                                                //throw new Exception(string.Format("No Card Mapping for this  Brand:{0},  Type:{1},  Level:{2}.", binlistdata.CardBrand, binlistdata.CardTransType, binlistdata.CardLevel));
                                                cardmapId = 13;
                                            }
                                            foreach (var bankcardMapId in bankcardMapIds)
                                            {
                                                var billingFeeTypeIds = dc.LPS_BIL_CostProcBillingFeeRates.Where(r => r.CardMapId == cardmapId && r.MidMgmtId == cost_midMgmtId && r.FeeTypeId == billingmethodfeetypeid).Select(r => r.FeeTypeId).Distinct().ToList();

                                                //This is added for Interchange Fee
                                                if (billingFeeTypeIds.Contains(36) || billingFeeTypeIds.Contains(37))
                                                    billingFeeTypeIds.Add(30);

                                                if (billingFeeTypeIds == null || billingFeeTypeIds.Count == 0)
                                                    throw new Exception(string.Format("There is no Cost of process Fee Type setup for this  Merchant {0} and cardMap {1}.<br />", billingMerchant.CompanyName, bankcardMapId));
                                                foreach (var billingFeeTypeId in billingFeeTypeIds)
                                                {
                                                    feeCount += EfDailyStatementTieredFee.CalDailyStatementCostofProcessBillingFeeCount(dc, billingMerchant, site.MerchantGroup, site.Merchant_User_Id, currency, bankConnId, cost_midMgmtId, BinData.CardBrand, BinData.CardTransType, BinData.CardLevel, BinData.BinCountry, cardmapId, billingFeeTypeId, statementDate, sb, dailyStatement.DailyStatementId, statementStartDate, statementEndDate, timeOffset);
                                                }
                                            }

                                        }
                                        #endregion
                                        break;
                                }
                            }
                            #endregion
                        }
                        #endregion

                        #region Step 7:Update daily statement details.
                        if (dailyStatement != null)
                        {
                            //Reserves                      
                            dailyStatement.ReserveHeld = (decimal)EfMerchantReserve.GetDailyReserve(dailyStatement.BillingMerchantId, dailyStatement.MerchantSiteId, dailyStatement.BankConnId, dailyStatement.CurrencyCode, dailyStatement.DailyStatementId, statementDate);
                            dailyStatement.ReserveRelease = (decimal)EfMerchantReserve.GetDailyRepaidReserve(dailyStatement.BillingMerchantId, dailyStatement.MerchantSiteId, dailyStatement.BankConnId, dailyStatement.CurrencyCode, dailyStatement.DailyStatementId, statementDate);

                            //Manual Adjustment
                            var dc2 = new LpsNetBilling2Entities();
                            var manualAdjustment = dc2.LPS_BIL_MerchantManualAdjustments.Where(s => s.BillingMerchantId == billingMerchant.BillingMerchantId && s.BankId == bankId && s.Currency == currency && s.AdjustmentDate.Year == statementDate.Year && s.AdjustmentDate.Month == statementDate.Month && s.AdjustmentDate.Day == statementDate.Day && s.Status == 1);
                            var adjustedCredit = manualAdjustment.Any(m => m.AccountEntryTypeId == 1) ? manualAdjustment.Where(m => m.AccountEntryTypeId == 1).Sum(t => (decimal)t.Amount) : 0;
                            foreach (var adjustedCreditAmount in manualAdjustment.Where(m => m.AccountEntryTypeId == 1))
                            {
                                adjustedCreditAmount.Status = 0;
                            }
                            var adjustedDebit = manualAdjustment.Any(m => m.AccountEntryTypeId == 2) ? manualAdjustment.Where(m => m.AccountEntryTypeId == 2).Sum(t => (decimal)t.Amount) : 0;
                            foreach (var adjustedDebitAmount in manualAdjustment.Where(m => m.AccountEntryTypeId == 2))
                            {
                                adjustedDebitAmount.Status = 0;
                            }
                            dc2.SaveChanges();
                            dailyStatement.ManualAdjustCredit = (decimal)adjustedCredit;
                            dailyStatement.ManualAdjustDebit = (decimal)adjustedDebit;
                            dailyStatement.PaymentPaid = (decimal)0.00;

                            //starting and ending balance
                            var Credit = dailyStatement.ReserveRelease + dailyStatement.ManualAdjustCredit;
                            var Debit = dailyStatement.ReserveHeld + dailyStatement.ManualAdjustDebit;

                            dailyStatement.EODBalance = EfDailyStatementTieredFee.GetEndofBalance(dc, billingMerchant, site.Merchant_User_Id, bankConnId, currency, dailyStatement.DailyStatementId, statementDate, Credit, Debit);
                            dailyStatement.BODBalance = EfDailyStatementTieredFee.GetBeginBalance(dc, billingMerchant, site.Merchant_User_Id, bankConnId, currency, dailyStatement.DailyStatementId, statementDate);

                            //Settlement
                            var dc1 = new LpsNetBilling2Entities();
                            var settledAmounts = dc1.LPS_BIL_MerchantSettlements.Where(s => s.BillingMerchantId == billingMerchant.BillingMerchantId && s.BankId == bankId && s.CurrencyCode == currency && s.SettleDate.Value.Year == statementDate.Year && s.SettleDate.Value.Month == statementDate.Month && s.SettleDate.Value.Day == statementDate.Day && s.Status == 3);
                            var settled = settledAmounts.Any() ? settledAmounts.Sum(t => (decimal)t.SettlementAmount) : 0;
                            foreach (var settledAmount in settledAmounts)
                            {
                                settledAmount.Status = 0;
                            }
                            dc1.SaveChanges();

                            dailyStatement.PaymentPaid = settled; //Added 

                            //Merchant Funds
                            var MerchantFunds = (decimal)EfMerchantFund.GetandUpdateMerchantFunds(dailyStatement.BillingMerchantId, dailyStatement.BankConnId, dailyStatement.CurrencyCode, dailyStatement.DailyStatementId, statementDate);

                            //AB
                            dailyStatement.CumulativeReserve = dailyStatement.ReserveHeld - dailyStatement.ReserveRelease + EfDailyStatementTieredFee.GetCumulativeReserveBalance(dc, billingMerchant, site.Merchant_User_Id, bankConnId, currency, dailyStatement.DailyStatementId, statementDate);
                            dailyStatement.CumulativeBalance = dailyStatement.BODBalance + dailyStatement.EODBalance + MerchantFunds - settled;

                            if (MerchantFunds == (decimal)0.00) //17Aug2023
                                dailyStatement.AvailableBalance = EfDailyStatementTieredFee.GetAvailableBalance(dc, billingMerchant, site.Merchant_User_Id, bankConnId, currency, dailyStatement.DailyStatementId, dailyStatement.CumulativeBalance, statementDate, IsSettledPerSite, ignoreDuplicateSettlement);
                            else
                                dailyStatement.AvailableBalance = EfDailyStatementTieredFee.GetAvailableBalanceandMerchantFund(dc, billingMerchant, site.Merchant_User_Id, bankConnId, currency, dailyStatement.DailyStatementId, dailyStatement.CumulativeBalance, statementDate, IsSettledPerSite, ignoreDuplicateSettlement);

                        }

                        #endregion

                        dc.SaveChanges();
                        // statementCount++;
                    }
                    if (merSites.Select(m => m.CurrencyCode).Distinct().ToList().Count() == 1)
                        IsSettledPerSite++; //To avoid duplicate settlement per site.
                }
                //if (merSites.Count == 0)
                //    InsertLog(billingMerchant.BillingMerchantId, bankConnId, statementDate, 2, dc);
                return statementCount;
            }
        }


    }
}
