import argparse
import glob
import os
import pandas as pd
import pandasql as ps


def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    #parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    #parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    #parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())


def main():
    cust_data = pd.read_csv("customers.csv")
    # print(cust_data)
    prod_data = pd.read_csv("products.csv")
    # print(prod_data)
    path_to_json = "transactions/*"
    json_pattern = os.path.join(path_to_json, '*.json')
    file_list = glob.glob(json_pattern)
    dfs = []  # an empty list to store the data frames
    for file in file_list:
        data = pd.read_json(file, lines=True)  # read data frame from json file
        dfs.append(data)  # append the data frame to the list

    trans_data = pd.concat(dfs, ignore_index=True)
    length = len(trans_data)

    def getProductsCount(start, end):

        for i in range(start, end):

            temp = trans_data['basket'][i]
            t2 = pd.DataFrame(data=temp, columns=["product_id"])

            #print(t2)

            q2 = """select product_id , count(*) cts from t2 GROUP BY product_id"""
            res = ps.sqldf(q2)
            #print(res)
            return res

    final_res2 = pd.DataFrame([])
    for ind in range(0, length):
        start = ind
        end = start+1
        temp1 = trans_data['customer_id'].loc[start]

        #print(temp1)

        temp2 = dict({"customer_id": temp1})
        trans_data1 = pd.DataFrame(temp2, index=[0])


        df_count = getProductsCount(start, end)

        final_res = """ select distinct customer_id, loyalty_score,p.product_id,p.product_category, c.cts purchase_count
                        from cust_data
                        join (select product_id, product_category from prod_data where
                                                            product_id in (select product_id from df_count)) p
				join (select product_id, cts from df_count where product_id in (select distinct(product_id) from df_count)) c
                                                            where customer_id in 
                        (select customer_id from trans_data1) group by customer_id, p.product_id
                                """
        #print(ps.sqldf(final_res))

        final_res1 = pd.DataFrame(ps.sqldf(final_res))
        final_res2 = final_res2.append(final_res1)
    #print(final_res2)
    final_res2.reset_index(inplace=True)
    final_res3 = """select customer_id, loyalty_score, product_id, product_category,
                            count(*) purchase_count from final_res2 group by customer_id, product_id"""

    final_res4 = pd.DataFrame(ps.sqldf(final_res3))
    final_res4.to_json('output.json', orient='records')


if __name__ == "__main__":
    main()
