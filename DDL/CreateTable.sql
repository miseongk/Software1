CREATE TABLE IF NOT EXISTS company_info (
    code VARCHAR(20),
    company VARCHAR(40),
    last_update DATE,
    PRIMARY KEY (code)
);

CREATE TABLE IF NOT EXISTS daily_price (
    open BIGINT,
    high BIGINT,
    low BIGINT,
    close BIGINT,
    volume BIGINT,
    change_ FLOAT,
    code VARCHAR(10),
    name TEXT,
    date DATE
);

CREATE TABLE IF NOT EXISTS krx_income_statement (
    stock_code VARCHAR(10),
    period VARCHAR(20),
    revenue FLOAT,
    cost_of_goods_sold FLOAT,
    gross_profit FLOAT,
    sales_general_administrative_exp_total FLOAT,
    operating_profit_total FLOAT,
    financial_income_total FLOAT,
    financial_costs_total FLOAT,
    other_income_total FLOAT,
    other_costs_total FLOAT,
    subsidiaries_jointVentures_pl_total FLOAT,
    ebit FLOAT,
    income_taxes_exp FLOAT,
    profit_cont_operation FLOAT,
    profit_discont_operation FLOAT,
    net_income_total FLOAT,
    net_income_controlling FLOAT,
    net_income_noncontrolling FLOAT,
    rpt_type VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS krx_balance_sheet (
    stock_code VARCHAR(10),
    period VARCHAR(20),
    assets_total FLOAT,
    current_assets_total FLOAT,
    lt_assets_total FLOAT,
    other_fin_assets FLOAT,
    liabilities_total FLOAT,
    current_liab_total FLOAT,
    lt_liab_total FLOAT,
    other_fin_liab_total FLOAT,
    equity_fin_liab_total FLOAT,
    equity_total FLOAT,
    paid_in_capital FLOAT,
    contingent_convertible_bonds FLOAT,
    capital_surplus FLOAT,
    other_equity FLOAT,
    accum_other_comprehensive_income FLOAT,
    retained_earnings FLOAT,
    rpt_type VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS krx_cash_flow (
    stock_code VARCHAR(10),
    period VARCHAR(20),
    CFO_Total FLOAT,
    Net_Income_Total FLOAT,
    Cont_Biz_Before_Tax FLOAT,
    Add_Exp_WO_CF_Out FLOAT,
    Ded_Rev_WO_CF_In FLOAT,
    Chg_Working_Capital FLOAT,
    CFO FLOAT,
    Other_CFO FLOAT,
    CFI_Total FLOAT,
    CFI_In FLOAT,
    CFI_Out FLOAT,
    Other_CFI FLOAT,
    CFF_Total FLOAT,
    CFF_In FLOAT,
    CFF_Out FLOAT,
    Other_CFF FLOAT,
    Other_CF FLOAT,
    Chg_CF_Consolidation FLOAT,
    Forex_Effect FLOAT,
    Chg_Cash_and_Cash_Equivalents FLOAT,
    Cash_and_Cash_Equivalents_Beg FLOAT,
    Cash_and_Cash_Equivalents_End FLOAT,
    rpt_type VARCHAR(20)
);