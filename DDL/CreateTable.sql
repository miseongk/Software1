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
    cfo_total FLOAT,
    net_income_total FLOAT,
    cont_biz_before_tax FLOAT,
    add_exp_wo_cf_out FLOAT,
    ded_rev_wo_cf_in FLOAT,
    chg_working_capital FLOAT,
    cfo FLOAT,
    other_cfo FLOAT,
    cfi_total FLOAT,
    cfi_in FLOAT,
    cfi_out FLOAT,
    other_cfi FLOAT,
    cff_total FLOAT,
    cff_in FLOAT,
    cff_out FLOAT,
    other_cff FLOAT,
    other_cf FLOAT,
    chg_cf_consolidation FLOAT,
    forex_effect FLOAT,
    chg_cash_and_cash_equivalents FLOAT,
    cash_and_cash_equivalents_beg FLOAT,
    cash_and_cash_equivalents_end FLOAT,
    rpt_type VARCHAR(20)
);