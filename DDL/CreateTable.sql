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