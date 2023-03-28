from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.decorators import dag, task
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from time import sleep
import time
import pandas as pd





default_args = {
    'owner':'sk',
    'retries':5,
        }



with DAG(
    dag_id ='webscraper_dag_test',
    default_args = {
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    },
    description='Our first airflow DAG',
    start_date=datetime(2023, 10, 1,1),
    schedule=timedelta(days=1),
    
    ) as dag:
            
        
        @task
        def write_data_to_csv(product_df):
            #results_df
            print("test ")    
            print(product_df)
            product_df.to_csv('Products_1_csv.csv', sep='\t', encoding='utf-8')    
            print("Done !")
            
       
    
        @task.virtualenv
        def python_scraper_func():
        #arg1, arg2, arg3, arg4

            import re 
            import selenium
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.common.keys import Keys
            from selenium.common.exceptions import NoSuchElementException
            from time import sleep
            from bs4 import BeautifulSoup
            import time
            import pandas as pd
            import requests
            
            time.sleep(3)


            urls_pages = []
            product_full_desc = []
            product_prices = []
            product_titles = []
            product_urls = []


            url_pg1 = "https://www.amazon.com/s?k=iphone"
            urls_pages.append(url_pg1)
        
            driver = webdriver.Chrome()
            driver.get(url_pg1)
                            
            """ 
            1. page 1 - collect data

            """
            ## data asin -- products in the whole page and print elements 
            p_data_asin_1 = driver.find_elements(By.CSS_SELECTOR,"#search > div.s-desktop-width-max.s-desktop-content.s-wide-grid-style-t1.s-opposite-dir.s-wide-grid-style.sg-row > div.sg-col-20-of-24.s-matching-dir.sg-col-16-of-20.sg-col.sg-col-8-of-12.sg-col-12-of-16 > div > span.rush-component.s-latency-cf-section > div.s-main-slot.s-result-list.s-search-results.sg-row > div:nth-child(3)")
            ## full text of product 
            print(p_data_asin_1[0].text)
            product_full_desc.append(p_data_asin_1[0].text)


            ## Product title 
            p1_title = driver.find_element(By.CSS_SELECTOR,"#search > div.s-desktop-width-max.s-desktop-content.s-wide-grid-style-t1.s-opposite-dir.s-wide-grid-style.sg-row > div.sg-col-20-of-24.s-matching-dir.sg-col-16-of-20.sg-col.sg-col-8-of-12.sg-col-12-of-16 > div > span.rush-component.s-latency-cf-section > div.s-main-slot.s-result-list.s-search-results.sg-row > div:nth-child(3) > div > div > div > div > div > div > div > div.sg-col.sg-col-4-of-12.sg-col-8-of-16.sg-col-12-of-20.sg-col-12-of-24.s-list-col-right > div > div > div.a-section.a-spacing-none.puis-padding-right-small.s-title-instructions-style > h2 > a")
            #print(type(p1_link))
            print("Product url 1 ",p1_title.get_attribute('href'))
            product_urls.append(p1_title.get_attribute('href'))
            print("Product title 1", p1_title.text)
            product_titles.append(p1_title.text)
            print()


            ##Product price 
            p1_price = driver.find_element(By.CSS_SELECTOR,"#search > div.s-desktop-width-max.s-desktop-content.s-wide-grid-style-t1.s-opposite-dir.s-wide-grid-style.sg-row > div.sg-col-20-of-24.s-matching-dir.sg-col-16-of-20.sg-col.sg-col-8-of-12.sg-col-12-of-16 > div > span.rush-component.s-latency-cf-section > div.s-main-slot.s-result-list.s-search-results.sg-row > div:nth-child(3) > div > div > div > div > div > div > div > div.sg-col.sg-col-4-of-12.sg-col-8-of-16.sg-col-12-of-20.sg-col-12-of-24.s-list-col-right > div > div > div.sg-row > div.sg-col.sg-col-4-of-12.sg-col-4-of-16.sg-col-4-of-20.sg-col-4-of-24 > div > div.a-section.a-spacing-none.a-spacing-top-micro.s-price-instructions-style > div > a > span:nth-child(1) > span.a-offscreen")
            print("Price of prod 1", p1_price.text)
            product_prices.append(p1_price.text)



            time.sleep(2)

            p_data_asin_2 = driver.find_element(By.CSS_SELECTOR,"#search > div.s-desktop-width-max.s-desktop-content.s-wide-grid-style-t1.s-opposite-dir.s-wide-grid-style.sg-row > div.sg-col-20-of-24.s-matching-dir.sg-col-16-of-20.sg-col.sg-col-8-of-12.sg-col-12-of-16 > div > span.rush-component.s-latency-cf-section > div.s-main-slot.s-result-list.s-search-results.sg-row > div:nth-child(4)")
            print("text of product 2", p_data_asin_2.text)
            product_full_desc.append(p_data_asin_2.text)

            ## Product title 
            p2_title = driver.find_element(By.CSS_SELECTOR,"#search > div.s-desktop-width-max.s-desktop-content.s-wide-grid-style-t1.s-opposite-dir.s-wide-grid-style.sg-row > div.sg-col-20-of-24.s-matching-dir.sg-col-16-of-20.sg-col.sg-col-8-of-12.sg-col-12-of-16 > div > span.rush-component.s-latency-cf-section > div.s-main-slot.s-result-list.s-search-results.sg-row > div:nth-child(4) > div > div > div > div > div > div > div > div.sg-col.sg-col-4-of-12.sg-col-8-of-16.sg-col-12-of-20.sg-col-12-of-24.s-list-col-right > div > div > div.a-section.a-spacing-none.puis-padding-right-small.s-title-instructions-style > h2 > a")
            #print(type(p1_link))
            print("Product url 2 ",p2_title.get_attribute('href'))
            product_urls.append(p2_title.get_attribute('href'))
            print("Product title 2", p2_title.text)
            product_titles.append(p2_title.text)
            print()


            ##Product price 
            p2_price = driver.find_element(By.CSS_SELECTOR,"#search > div.s-desktop-width-max.s-desktop-content.s-wide-grid-style-t1.s-opposite-dir.s-wide-grid-style.sg-row > div.sg-col-20-of-24.s-matching-dir.sg-col-16-of-20.sg-col.sg-col-8-of-12.sg-col-12-of-16 > div > span.rush-component.s-latency-cf-section > div.s-main-slot.s-result-list.s-search-results.sg-row > div:nth-child(4) > div > div > div > div > div > div > div > div.sg-col.sg-col-4-of-12.sg-col-8-of-16.sg-col-12-of-20.sg-col-12-of-24.s-list-col-right > div > div > div.sg-row > div.sg-col.sg-col-4-of-12.sg-col-4-of-16.sg-col-4-of-20.sg-col-4-of-24 > div > div.a-section.a-spacing-none.a-spacing-top-micro.s-price-instructions-style > div > a > span:nth-child(1)")
            print("Price of prod 2", p2_price.text)
            product_prices.append(p2_price.text)

            for item in range(7,23):
                print()

                p_desc_selector = "#search > div.s-desktop-width-max.s-desktop-content.s-wide-grid-style-t1.s-opposite-dir.s-wide-grid-style.sg-row > div.sg-col-20-of-24.s-matching-dir.sg-col-16-of-20.sg-col.sg-col-8-of-12.sg-col-12-of-16 > div > span.rush-component.s-latency-cf-section > div.s-main-slot.s-result-list.s-search-results.sg-row > div:nth-child(" + str(item) + ")"

                p_title_selector = p_desc_selector + "> div > div > div > div > div > div.sg-col.sg-col-4-of-12.sg-col-8-of-16.sg-col-12-of-20.sg-col-12-of-24.s-list-col-right > div > div > div.a-section.a-spacing-none.puis-padding-right-small.s-title-instructions-style > h2 > a"
                p_price_selector = "#search > div.s-desktop-width-max.s-desktop-content.s-wide-grid-style-t1.s-opposite-dir.s-wide-grid-style.sg-row > div.sg-col-20-of-24.s-matching-dir.sg-col-16-of-20.sg-col.sg-col-8-of-12.sg-col-12-of-16 > div > span.rush-component.s-latency-cf-section > div.s-main-slot.s-result-list.s-search-results.sg-row > div:nth-child("+ str(item) + ") > div > div > div > div > div > div.sg-col.sg-col-4-of-12.sg-col-8-of-16.sg-col-12-of-20.sg-col-12-of-24.s-list-col-right > div > div > div.sg-row > div.sg-col.sg-col-4-of-12.sg-col-4-of-16.sg-col-4-of-20.sg-col-4-of-24 > div > div.a-section.a-spacing-none.a-spacing-top-micro.s-price-instructions-style > div > a > span:nth-child(1) "

                try:
                    ## full description of product
                    p_desc = driver.find_elements(By.CSS_SELECTOR,p_desc_selector)
                    print(p_desc[0].text)
                    product_full_desc.append(p_desc[0].text)
                    
                    ## title 
                    p_title = driver.find_elements(By.CSS_SELECTOR,p_title_selector)
                    print(p_title)
                    product_titles.append(p_title)
                    if len(p_title) > 0:
                        print(p_title[0].get_attribute('href'))
                        product_urls.append(p_title[0].get_attribute('href'))
                    
                    
                    ##price
                    p_price = driver.find_element(By.CSS_SELECTOR, p_price_selector)
                    print(p_price.text)
                    product_prices.append(p_price.text)
                        
                except NoSuchElementException:
                    p_title = "NaN"
                    pass

                time.sleep(3)
                        
            ##wait before page 2
            driver.implicitly_wait(5)
            time.sleep(3)
            
            
            """ 
            Page 2 

            """
            """
            2. Page 2 section -- get page 2 section details 

            """
         
            page_2 = driver.find_element(By.XPATH,'//*[@id="search"]/div[1]/div[1]/div/span[1]/div[1]/div[25]/div/div/span/a[1]')

            #print("print page2",page_2)
            print("page_2 url", page_2.get_attribute('href'))

            page_2_url = page_2.get_attribute('href')
            print("url of page2 ",page_2_url)

            urls_pages.append(page_2_url)

            driver.get(page_2_url)
            


            """
            3. DONE: creating the next page urls from pagination

            """
            num = 3
            test_url = page_2_url
            print(test_url)

            first = re.split('=2',test_url)
            print(first)

            t = first[0]
            ##part1 of url
            t1 = t + '='
            t_1 = t + '=' + str(num) 

            str_2 = first[1].strip('_2')
            ## part 2 of url
            t2 = str_2 + '_'
            t_2 = str_2 + '_' + str(num)
            next_page = t_1 + t_2

            print(next_page)

            urls_pages.append(next_page)

            """ 
            4. Checks if url is opening or not and collect urls of next pages 

            """

            def check_url_status_of_next_pages(t1,t2,urls_pages):

                #total_pgs = 20

                for num in range(3,21):
                    #url_str = "https://www.amazon.com/s?k=iphone&page="+ str(num) +"&qid=1678848977&ref=sr_pg_" + str(num)

                    url_str = t1 + str(num) + t2 + str(num)
                    print("next page url in loop" , url_str)
                    
                    try:
                        #driver.get(url_str)
                        get = requests.get(url_str)
                        ## adding urls to list 
                        urls_pages.append(url_str)
                        time.sleep(5)
                        print("test url opening done",url_str)
                        
                        # if the request succeeds 
                        if get.status_code == 200:
                            print(f"{url_str}: url works")
                        else:
                            print(f"{url_str}: does not, status_code: {get.status_code}")
                            
                    except requests.exceptions.RequestException as e:
                        ## print url with exception
                        print(url_str, e)
                        url_str = "NaN"
                        print("Url doesn't work")

                    

            #time.sleep(5)

            check_url_status_of_next_pages(t1,t2,urls_pages)

            ## collected urls 
            print(urls_pages)



                
            """
            5. Function to grab elements from page 2 till last page 

            """    
            


            for url_ in urls_pages[1:-1]:
                print(url_)
                driver.get(url_)
            
                for item in range(6,22):
                    print()
                    p_desc_selector = "#search > div.s-desktop-width-max.s-desktop-content.s-wide-grid-style-t1.s-opposite-dir.s-wide-grid-style.sg-row > div.sg-col-20-of-24.s-matching-dir.sg-col-16-of-20.sg-col.sg-col-8-of-12.sg-col-12-of-16 > div > span.rush-component.s-latency-cf-section > div.s-main-slot.s-result-list.s-search-results.sg-row > div:nth-child(" + str(item) + ")"

                    p_title_selector = p_desc_selector + "> div > div > div > div > div > div.sg-col.sg-col-4-of-12.sg-col-8-of-16.sg-col-12-of-20.sg-col-12-of-24.s-list-col-right > div > div > div.a-section.a-spacing-none.puis-padding-right-small.s-title-instructions-style > h2 > a"
                    p_price_selector = "#search > div.s-desktop-width-max.s-desktop-content.s-wide-grid-style-t1.s-opposite-dir.s-wide-grid-style.sg-row > div.sg-col-20-of-24.s-matching-dir.sg-col-16-of-20.sg-col.sg-col-8-of-12.sg-col-12-of-16 > div > span.rush-component.s-latency-cf-section > div.s-main-slot.s-result-list.s-search-results.sg-row > div:nth-child("+ str(item) + ") > div > div > div > div > div > div.sg-col.sg-col-4-of-12.sg-col-8-of-16.sg-col-12-of-20.sg-col-12-of-24.s-list-col-right > div > div > div.sg-row > div.sg-col.sg-col-4-of-12.sg-col-4-of-16.sg-col-4-of-20.sg-col-4-of-24 > div > div.a-section.a-spacing-none.a-spacing-top-micro.s-price-instructions-style > div > a > span:nth-child(1) "
                    
                    try:
                        ## full description of product
                        p_desc = driver.find_elements(By.CSS_SELECTOR,p_desc_selector)
                        print(p_desc[0].text)
                        product_full_desc.append(p_desc[0].text)
                        
                        ## title 
                        p_title = driver.find_elements(By.CSS_SELECTOR,p_title_selector)
                        #if len(p_title) > 0:
                        print(p_title)
                        product_titles.append(p_title)
                        print(type(p_title))
                        try:
                            print(p_title[0].get_attribute('href'))
                            product_urls.append(p_title[0].get_attribute('href'))
                        
                        except Exception as e:
                            print(e)
                            pass
                        
                        ##price
                        p_price = driver.find_element(By.CSS_SELECTOR, p_price_selector)
                        print(p_price.text)
                        product_prices.append(p_price.text)
                            
                    except NoSuchElementException:
                        p_title = "NaN"
                        pass
                    
                time.sleep(5)
                
                

            """ 
            6. Print results and write to csv

            """
            print("Product description",product_full_desc)
            print()
            print("product titles",product_titles)
            print()
            print("product prices",product_prices)   

            product_df = pd.DataFrame({'Product titles':pd.Series(product_titles),
                                    'Product prices' :pd.Series(product_prices),
                                    'Product desc':pd.Series(product_full_desc),
                                    'Product urls' : pd.Series(product_urls)})

            print(product_df)

            print("Done!")
            
            return product_df
                                    
        
        
        
        

        venv_operator = PythonVirtualenvOperator(
        task_id='python-virtual-env-demo',
        python_callable=python_scraper_func,
        #op_args=[arg1, arg2, arg3, arg4],
        requirements=["selenium ~= 4.8",
                      "requests",
                      "pandas"],
        python_version='3',
        system_site_packages=False,
        dag=dag)

        venv_operator >> write_data_to_csv
       
             
      
      
  