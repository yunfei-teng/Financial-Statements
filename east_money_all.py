import pandas as pd
import akshare as ak

class DataScraperAKShare:
    def __init__(self):
        """ requires `pip install akshare --upgrad``"""
        """ refer to: https://akshare.akfamily.xyz/tutorial.html """
        # 股票-三大报表
        # "stock_balance_sheet_by_report_em"    # 东方财富-股票-财务分析-资产负债表-按报告期
        # "stock_profit_sheet_by_report_em"     # 东方财富-股票-财务分析-利润表-报告期
        # "stock_cash_flow_sheet_by_report_em"  # 东方财富-股票-财务分析-现金流量表-按报告期

    def run(self):
        self.query_type = str(input('请输入查询表格(1-资产负债表；2-利润表；3-现金流量表): '))
        if 1 <= int(self.query_type) <=3:
            self.query_type =  ['资产负债表', '利润表', '现金流量表'][int(self.query_type) - 1]
        if self.query_type == '资产负债表':
            get_table = ak.stock_cash_flow_sheet_by_yearly_em
        elif self.query_type == '利润表':
            get_table = ak.stock_profit_sheet_by_report_em
        elif self.query_type == '现金流量表':
            get_table = ak.stock_cash_flow_sheet_by_report_em
        else:
            raise ValueError('该表没有定义!')

        stock_hot_deal_xq_df = pd.read_csv('docs/stock_hot_deal_xq.csv')
        for i, stock_name in enumerate(stock_hot_deal_xq_df['股票简称']):
            print(f"[{i + 1: 4d}/{len(stock_hot_deal_xq_df['股票简称']): 4d}] Finished Processing: {stock_name}")
            self.stock_code = stock_hot_deal_xq_df.loc[stock_hot_deal_xq_df['股票简称'] == stock_name]['股票代码'].iloc[0]
            df = get_table(symbol=self.stock_code)
            df.to_csv(f"saved_tables/csv/{self.query_type}_{stock_name}_{self.stock_code}.csv")
            df.to_excel(f"saved_tables/xlsx/{self.query_type}_{stock_name}_{self.stock_code}.xlsx")

if __name__ == '__main__':
    scraper = DataScraperAKShare()
    scraper.run()