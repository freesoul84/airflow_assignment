import pandas as pd
import logging

def data_cleaning(tbl, tbl_df):
    """
    Function will clean the data to keep it accurate
    """
    logging.info(f"Data cleaning activity for {tbl}")
    logging.info(f"Remove duplicate rows")
    tbl_df.drop_duplicates(inplace = True)
    logging.info("Remove rows having primary key as an id column null values")
    tbl_df.dropna(subset = [col for col in tbl_df.columns if col.endswith('_id')],inplace=True)
    logging.info("Fill missing values for other columns")
    tbl_df.fillna(tbl_df.mean(numeric_only = True), inplace = True)

    # wherever id column is there convert it to integer column
    for col_name in tbl_df.columns:
        if col_name.endswith("_id"):
            tbl_df[col_name] = tbl_df[col_name].astype('int64')

    return tbl_df

def data_transformation(tbl_dictionary):
    """
    Function will do data transformation
    """
    logging.info("start the data cleaning activity")
    cleaned_dfs = {}
    for tbl,tbl_df in tbl_dictionary.items():
        cleaned_dfs[tbl] = data_cleaning(tbl, tbl_df)
    
    tbl_dictionary = cleaned_dfs
    logging.info("start merging tables to find data insights")
    customers_df = tbl_dictionary['customers']
    orders_df = tbl_dictionary['orders']
    order_items_df = tbl_dictionary['order_items']
    categories_df = tbl_dictionary['categories']
    reviews_df = tbl_dictionary['reviews']
    products_df = tbl_dictionary['products']

    logging.info("Top 5 customers by total amount spend")
    top_customer = orders_df.groupby("customer_id").agg({'total_amount':'sum'}).sort_values(by='total_amount',ascending = False).head(5)
    
    logging.info("Top 5 products by number of orders")
    top_products = order_items_df.groupby("product_id").agg({'quantity':'sum'}).sort_values(by='quantity',ascending = False).head(5)

    logging.info("Average rating of products by category.")
    avg_rating_df = products_df.merge(categories_df, on = "category_id").merge(reviews_df, on = "product_id")
    print(avg_rating_df.columns)
    avg_rating_df = avg_rating_df.groupby("category_id")['rating'].mean()

    #products_join_df = orders_df.merge(order_items_df, on = "order_id").merge(products_df, on = "product_id").merge(categories_df, on = "category_id").merge(reviews_df, on = "product_id")
    #customers_join_df = orders_df.merge(order_items_df, on = "order_id").merge(products_df, on = "product_id").merge(categories_df, on = "category_id").merge(reviews_df, on = "product_id").merge(customers_df, merge='customer_id')

    return {'data_transform' : tbl_dictionary,'data_insights': {'top_products' : top_products,
                                                'top_customer' : top_customer,
                                                'avg_rating_df' : avg_rating_df}}
