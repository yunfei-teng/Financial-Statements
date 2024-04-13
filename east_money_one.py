import csv
import json
import requests
import pandas as pd
import akshare as ak

from lxml import etree
from datetime import datetime

class DataScraperDataCenter:
    def __init__(self):
        self.pagename_type = {
            "业绩报表": "RPT_LICO_FN_CPD",
            "业绩快报": "RPT_FCI_PERFORMANCEE",
            "业绩预告": "RPT_PUBLIC_OP_NEWPREDICT",
            "预约披露时间": "RPT_PUBLIC_BS_APPOIN",
            "资产负债表": "RPT_DMSK_FN_BALANCE",
            "利润表": "RPT_DMSK_FN_INCOME",
            "现金流量表": "RPT_DMSK_FN_CASHFLOW"
        }

        self.pagename_en = {
            "业绩报表": "yjbb",
            "业绩快报": "yjkb",
            "业绩预告": "yjyg",
            "预约披露时间": "yysj",
            "资产负债表": "zcfz",
            "利润表": "lrb",
            "现金流量表": "xjll"
        }

        self.en_list = []
        self.url = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
        self.headers = {
            'Accept': '*/*',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'closed',
            'Referer': 'https://data.eastmoney.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }

    def get_table(self, page):
        params = {
            'sortTypes': '-1,-1',
            'reportName': self.table_type,
            'columns': 'ALL',
            'filter': f'(REPORT_DATE=\'{self.timePoint}\')'
        }

        if self.table_type in ['RPT_LICO_FN_CPD']:
            params['filter'] = f'(REPORTDATE=\'{self.timePoint}\')'
        params['pageNumber'] = str(page)
        response = requests.get(url=self.url, params=params, headers=self.headers)
        data = json.loads(response.text)
        if data['result']:
            return data['result']['data']
        else:
            return

    def get_header(self, all_en_list):
        ch_list = []
        url = f'https://data.eastmoney.com/bbsj/{self.pagename_en[self.pagename]}.html'
        response = requests.get(url)
        res = etree.HTML(response.text)
        for en in all_en_list:
            ch = ''.join(
                [i.strip() for i in res.xpath(f'//div[@class="dataview"]//table[1]//th[@data-field="{en}"]//text()')])
            if ch:
                ch_list.append(ch)
                self.en_list.append(en)
        # print(len(ch_list), ch_list)
        return ch_list

    def write_header(self, table_data):
        with open(self.filename, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            headers = self.get_header(list(table_data[0].keys()))
            writer.writerow(headers)

    def write_table(self, table_data):
        with open(self.filename, 'a', encoding='utf-8', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for item in table_data:
                row = []
                for key in item.keys():
                    if key in self.en_list:
                        row.append(str(item[key]))
                print(row)
                writer.writerow(row)

    def get_timeList(self):
        ''' return a time list '''
        headers = {
            'Referer': 'https://data.eastmoney.com/bbsj/202206.html',
        }
        response = requests.get('https://data.eastmoney.com/bbsj/202206.html', headers=headers)
        res = etree.HTML(response.text)
        current_date = str(datetime.today().strftime('%Y-%m-%d'))
        return [date for date in res.xpath('//*[@id="filter_date"]//option/text()') if current_date > str(date)]

    def run(self):
        # Deal with time
        self.timeList = self.get_timeList()
        for index, value in enumerate(self.timeList):
            if (index + 1) % 10 == 0: print(value)
            else: print(value, end=' ; ')
        self.timePoint = str(input('\n请选择时间（可选项如上）:'))
        assert self.timePoint in self.timeList + [''], '时间输入错误'

        # Deal with type
        # print(self.pagename_type.keys())
        self.pagename = str(input('请输入报表类型（1-业绩报表; 2-业绩快报; 3-业绩预告; 4-预约披露时间; 5-资产负债表; 6-利润表；7-现金流量表）:'))
        if self.timePoint == '':
            self.timePoint = self.timeList[0] # 使用最新数据
        if 1 <= int(self.pagename) <= len(self.pagename_type):
            self.pagename = list(self.pagename_type.keys())[int(self.pagename)-1]
        assert self.pagename in list(self.pagename_type.keys()), '报表类型输入错误'
        print(f"===== Downloading {self.pagename} of data {self.timePoint} =====")

        self.table_type = self.pagename_type[self.pagename]
        self.filename = f'{self.pagename}_{self.timePoint}.csv'
        self.write_header(self.get_table(1))
        cur_page = 1
        while (table := self.get_table(cur_page)):
            self.write_table(table)
            cur_page += 1

class DataScraperAKShare:
    def __init__(self):
        """ requires `pip install akshare --upgrad``"""
        """ refer to: https://akshare.akfamily.xyz/tutorial.html """
        # 股票-三大报表
        # "stock_balance_sheet_by_report_em"    # 东方财富-股票-财务分析-资产负债表-按报告期
        # "stock_profit_sheet_by_report_em"     # 东方财富-股票-财务分析-利润表-报告期
        # "stock_cash_flow_sheet_by_report_em"  # 东方财富-股票-财务分析-现金流量表-按报告期

    def run(self):
        self.stock_name = str(input('请输入股票名称: '))
        stock_hot_deal_xq_df = pd.read_csv('docs/stock_hot_deal_xq.csv')
        assert stock_hot_deal_xq_df['股票简称'].isin([self.stock_name]).any().any(), '错误股票名称'
        self.stock_code = stock_hot_deal_xq_df.loc[stock_hot_deal_xq_df['股票简称'] == self.stock_name]['股票代码'].iloc[0]
        self.query_type = str(input('请输入查询表格(1-资产负债表；2-利润表；3-现金流量表): '))
        if 1 <= int(self.query_type) <=3:
             self.query_type =  ['资产负债表', '利润表', '现金流量表'][int(self.query_type) - 1]
        
        if self.query_type == '资产负债表':
            df = ak.stock_cash_flow_sheet_by_yearly_em(symbol=self.stock_code)
        elif self.query_type == '利润表':
            df = ak.stock_profit_sheet_by_report_em(symbol=self.stock_code)
        elif self.query_type == '现金流量表':
            df = ak.stock_cash_flow_sheet_by_report_em(symbol=self.stock_code)
        else:
            raise ValueError('该表没有定义!')

        df.to_csv(f"saved_tables/csv/{self.query_type}_{self.stock_name}_{self.stock_code}.csv")
        df.to_excel(f"saved_tables/xlsx/{self.query_type}_{self.stock_name}_{self.stock_code}.xlsx")


if __name__ == '__main__':
    scraper = DataScraperAKShare()
    scraper.run()