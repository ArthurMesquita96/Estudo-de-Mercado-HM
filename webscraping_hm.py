## Imports

import requests
import time
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import numpy as np
import re
import logging
import os
import pandas as pd
import sqlite3
from sqlalchemy import create_engine

def data_collection(url, headers):

    ## Data Collection
    # ----------------> Página de calças jeans masculinas na H&M

    page = requests.get(url, headers=headers)

    # Beautiful Soup objetc
    soup = BeautifulSoup(page.text, 'html.parser')

    # armazenando lista de produtos
    products = soup.find('ul', class_='products-listing small')

    # ----->  Coletando dados da Vitrine 

    # coletando os product_ids
    product_list = products.find_all('article', class_='hm-product-item')
    product_id = [p.get('data-articlecode') for p in product_list]

    # product_category
    product_category = [p.get('data-category') for p in product_list]

    # unificando dados em um data frame
    data_vitrine = pd.DataFrame([product_id, product_category]).T
    data_vitrine.columns = [ 'product_id', 'product_category']

    # adicionando feature de style_id
    data_vitrine['style_id'] = data_vitrine['product_id'].apply(lambda x: x[:-3]) 

    # logs
    logger.info( 'produtos da vitrine coletados com sucesso' )
    logger.debug( '%s produtos foram coletados', len(data_vitrine) )
    
    return data_vitrine

def data_collection_by_product(data):
    ## Data collect by product

    # df que armazenará o detalhe de todos os produtos
    df_products = pd.DataFrame()

    # Para cada produto na vitrine
    # Extrair todas as cores
    for i in range(len(data)):    

        # Request API
        url = 'https://www2.hm.com/en_us/productpage.'+ data.loc[i,'product_id'] +'.html'  
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

        logger.debug('Product: %s', url)

        # Request para a página específica do produto
        # No caso do falha, faz-se uma pausa antes de tentar novamente a request
        page = ''
        while page == '':
            try:
                page = requests.get(url,headers=headers)
                break
            except:
                print("Connection refused by the server..")
                print("Waiting 2 seconds")
                logger.debug('conexao da pagina do produto %s recusada. Tentando novamente em 2 segundos', data.loc[i,'product_id'])
                time.sleep(2)
                continue 

        # Beautiful object
        soup = BeautifulSoup(page.text,'html.parser')

        # ======================== Color code ============================

        # Coletar a cor dos produtos
        # caso a página do produto seja aberta mas não exista atributos descritos, o processo é ignorado
        try:
            # lista de produtos
            product_list = soup.find('ul',class_='inputlist clearfix').find_all('li',class_='list-item')

            # nome da cor de cada produto
            color_name = [p.find('a').get('data-color') for p in product_list]

            # product_id de cada produto (combinação entre style_id e color_id)
            product_id = [p.find('a').get('data-articlecode') for p in product_list]
        except:
            logger.degud('pagina do producto %s vazia, produto ignorado',data.loc[i,'product_id'])
            print('This page is empty')
            continue

        # criando o Data Frame com as cores
        df_colors_product = pd.DataFrame([product_id, color_name]).T
        df_colors_product.columns = ['product_id', 'color_name']

        # criando features para os ids de cor e estilo
        df_colors_product['color_id'] = df_colors_product['product_id'].apply(lambda x: x[-3::])
        df_colors_product['style_id'] = df_colors_product['product_id'].apply(lambda x: x[:-3])

        # ======================== Composition ============================

        # Para cada cor de produto na vitrine
        # Extrair a composição

        # df que armazenará as composições de cada cor
        df_compositions = pd.DataFrame()

        for j in range(len(df_colors_product)):    

            # Request API
            url = 'https://www2.hm.com/en_us/productpage.'+ df_colors_product.loc[j,'product_id'] +'.html'  
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

            logger.debug('Color: %s', url)

            page = ''
            while page == '':
                try: 
                    page = requests.get(url,headers=headers)
                    break
                except:
                    logger.debug('Conexao da pagina do produto %s recusada. Tentando novamente em 2 segundos', data.loc[i,'product_id'])
                    time.sleep(2)
                    pass

            # Beautiful object
            soup = BeautifulSoup(page.text,'html.parser')

            try: 

                # ======= Product name

                product_name = soup.find_all('h1',class_='primary product-item-headline')
                product_name = product_name[0].get_text().replace('\t','').replace('\n','')

                # ======= Product price

                product_price = soup.find('div',class_='primary-row product-item-price')
                product_price = product_price.find('span').get_text().replace('\r','').replace('\n','').replace(' ','')

                # ======= Product composition

                # lista de atributos do produto
                composition_soup = soup.find_all('div',class_="details-attributes-list-item")


                # nome dos atributos do produto
                composition_name = [composition_soup[i].find('dt').get_text().replace('messages.','') for i in range(len(composition_soup))]

                # reunindo todos os detalhes do produto em uma lista única
                # como diferentes productos possuem quantidades diferentes de atributos, é feito um join que 
                # reune todos em uma única coluna
                product_composition = [ 

                    [','.join(list(filter(None,p.get_text().split('\n')))[1::]) for p in composition_soup]
                                      ]

                # criando dataframe com todos os atributos
                df_composition = pd.DataFrame(product_composition,columns=composition_name)

                # adicionando features
                df_composition['product_name'] = product_name
                df_composition['product_price'] = product_price

                # renomeando feature para product_id
                df_composition = df_composition.rename(columns={'Art. No.':'product_id'})

                # dataframe que concatena as composições de todas as cores
                df_compositions = pd.concat([df_compositions,df_composition],axis=0,ignore_index=True)

            except:
                logger.degud('pagina do producto %s vazia, produto ignorado',data.loc[i,'product_id'])
                print('This page is broken')
                time.sleep(1)
                pass

        # ======================== Merging Dataframes ============================

        # merge do dataframe de cores com o de composições
        data_aux = pd.merge( df_compositions, df_colors_product,how ='left',on ='product_id')

        # dataframe que concatena os dados de todos os produtos
        df_products = pd.concat([df_products,data_aux],axis=0,ignore_index=True)

    # adicionando feature sobre a data/hora da coleta
    df_products['scrapy_datetime'] = datetime.now().strftime('%Y-%m-%d %H-%M-%S')

    # padronizando o nome das columas do df 
    df_products.columns = [c.lower() for c in df_products.columns]

    # organizando o df de forma mais intuitiva
    df_products = df_products[['product_id','product_name','style_id','color_id','color_name','product_price','fit','composition','size','scrapy_datetime']]

    # adicionando feature de product_category 
    df_products = pd.merge(df_products,data.drop('product_id',axis=1),how='left',on='style_id')

    # removendo duplicatas dos produtos
    df_products = df_products.drop_duplicates('product_id').reset_index(drop=True)

    # # salva em csv o df final
    # df_products.to_csv('products_hm.csv',index=False)

    # log
    logger.info('Detalhes dos produtos coletados com sucesso')
    logger.debug('Os detalhes de %s produtos foram coletados', len(df_products))
    
    return df_products

def data_cleaning(data):
    ## Data Cleaning

    # product_id
    # data = data.dropna(subset=['product_id'])
    data['product_id'] = data['product_id'].astype(str)

    # product_category

    # product_name
    # data['product_name'] = data['product_name'].apply(lambda x: x.replace(' ','').lower() if pd.notnull(x) else x )
    data['product_name'] = data['product_name'].str.replace('  ','')
    data['product_name'] = data['product_name'].str.replace(' ','_').str.lower()


    # product_price
    data['product_price'] = data['product_price'].apply(lambda x: x.replace('$','') if pd.notnull(x) else x)
    data['product_price'] = data['product_price'].astype(float)

    # scrapy_datetime
    data['scrapy_datetime'] = pd.to_datetime(data['scrapy_datetime'], format ='%Y-%m-%d %H-%M-%S')

    # style_id
    data['style_id'] = data['style_id'].astype(int)

    # color_id
    data['color_id'] = data['color_id'].astype(int)

    # color_name
    data['color_name'] = data['color_name'].apply(lambda x: x.replace(' ','_').replace('/','_').lower() if pd.notnull(x) else x)

    # fit
    data['fit'] = data['fit'].apply(lambda x: x.replace(' ','').lower() if pd.notnull(x) else x)

    # length
    data['size_number'] = data['size'].apply(lambda x: re.search('\d{2}\.\d{1}',x).group(0) if pd.notnull(x) else x)

    # size
    data['size_model'] = data['size'].str.extract('(\d+/\\d+)')

    df_ref = pd.DataFrame(index=np.arange(len(data)), columns=['cotton', 'polyester', 'spandex', 'elastomultiester',
                                                               'shell_cotton', 'shell_spandex', 'shell_polyester',
                                                               'shell_elastomultiester', 'pocket_lining_cotton',
                                                               'pocket_lining_spandex', 'pocket_lining_polyester',
                                                               'pocket_lining_elastomultiester'])

    df_composition = data[~data['composition'].str.contains('Pocket lining', na=False)]
    df_composition = df_composition[~df_composition['composition'].str.contains('Lining', na=False)]
    df_composition = df_composition[~df_composition['composition'].str.contains('Shell', na=False)]

    df_ref['cotton']                         = df_composition[df_composition['composition'].str.contains("Cotton")]['composition'].apply(lambda x: re.search('Cotton (.+?)%',x).group(1))
    df_ref['spandex']                        = df_composition[df_composition['composition'].str.contains("Spandex")]['composition'].apply(lambda x: re.search('Spandex (.+?)%',x).group(1))
    df_ref['polyester']                      = df_composition[df_composition['composition'].str.contains("Polyester")]['composition'].apply(lambda x: re.search('Polyester (.+?)%',x).group(1))
    df_ref['elastomultiester']               = df_composition[df_composition['composition'].str.contains("Elastomultiester")]['composition'].apply(lambda x: re.search('Elastomultiester (.+?)%',x).group(1))

    df_shell = data.loc[data['composition'].str.contains("Shell"),'composition']
    df_shell = df_shell.apply(lambda x: re.search('Shell: (.+?),Pocket',x).group(1))

    df_ref['shell_cotton']                   = df_shell[df_shell.str.contains("Cotton")].apply(lambda x: re.search('Cotton (.+?)%',x).group(1))
    df_ref['shell_spandex']                  = df_shell[df_shell.str.contains("Spandex")].apply(lambda x: re.search('Spandex (.+?)%',x).group(1))
    df_ref['shell_polyester']                = df_shell[df_shell.str.contains("Polyester")].apply(lambda x: re.search('Polyester (.+?)%',x).group(1))
    df_ref['shell_elastomultiester']         = df_shell[df_shell.str.contains("Elastomultiester")].apply(lambda x: re.search('Elastomultiester (.+?)%',x).group(1))

    df_pocket_lining = data.loc[data['composition'].str.contains("Pocket lining"),'composition']
    df_pocket_lining = df_pocket_lining.apply(lambda x: re.search('Pocket lining: (.+?)$',x).group(1))

    df_ref['pocket_lining_cotton']           = df_pocket_lining[df_pocket_lining.str.contains("Cotton")].apply(lambda x: re.search('Cotton (.+?)%',x).group(1))
    df_ref['pocket_lining_spandex']          = df_pocket_lining[df_pocket_lining.str.contains("Spandex")].apply(lambda x: re.search('Spandex (.+?)%',x).group(1))
    df_ref['pocket_lining_polyester']        = df_pocket_lining[df_pocket_lining.str.contains("Polyester")].apply(lambda x: re.search('Polyester (.+?)%',x).group(1))
    df_ref['pocket_lining_elastomultiester'] = df_pocket_lining[df_pocket_lining.str.contains("Elastomultiester")].apply(lambda x: re.search('Elastomultiester (.+?)%',x).group(1))


    df_ref['cotton'] = df_ref['cotton'].apply(lambda x: int(re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_ref['spandex'] = df_ref['spandex'].apply(lambda x: int(re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_ref['polyester'] = df_ref['polyester'].apply(lambda x: int(re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_ref['elastomultiester'] = df_ref['elastomultiester'].apply(lambda x: int(re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_ref['shell_cotton'] = df_ref['shell_cotton'].apply(lambda x: int(re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_ref['shell_spandex'] = df_ref['shell_spandex'].apply(lambda x: int(re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_ref['shell_polyester'] = df_ref['shell_polyester'].apply(lambda x: int(re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_ref['shell_elastomultiester'] = df_ref['shell_elastomultiester'].apply(lambda x: int(re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_ref['pocket_lining_cotton'] = df_ref['pocket_lining_cotton'].apply(lambda x: int(re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_ref['pocket_lining_spandex'] = df_ref['pocket_lining_spandex'].apply(lambda x: int(re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_ref['pocket_lining_polyester'] = df_ref['pocket_lining_polyester'].apply(lambda x: int(re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)
    df_ref['pocket_lining_elastomultiester'] = df_ref['pocket_lining_elastomultiester'].apply(lambda x: int(re.search('\d+',x).group(0)) / 100 if pd.notnull(x) else x)


    df_ref['cotton'] = df_ref['cotton'].fillna(0.00)
    df_ref['spandex'] = df_ref['spandex'].fillna(0.00)
    df_ref['polyester'] = df_ref['polyester'].fillna(0.00)
    df_ref['elastomultiester'] = df_ref['elastomultiester'].fillna(0.00)
    df_ref['shell_cotton'] = df_ref['shell_cotton'].fillna(0.00)
    df_ref['shell_spandex'] = df_ref['shell_spandex'].fillna(0.00)
    df_ref['shell_polyester'] = df_ref['shell_polyester'].fillna(0.00)
    df_ref['shell_elastomultiester'] = df_ref['shell_elastomultiester'].fillna(0.00)
    df_ref['pocket_lining_cotton'] = df_ref['pocket_lining_cotton'].fillna(0.00)
    df_ref['pocket_lining_spandex'] = df_ref['pocket_lining_spandex'].fillna(0.00)
    df_ref['pocket_lining_polyester'] = df_ref['pocket_lining_polyester'].fillna(0.00)
    df_ref['pocket_lining_elastomultiester'] = df_ref['pocket_lining_elastomultiester'].fillna(0.00)


    data = pd.concat([data,df_ref], axis=1)

    data = data[['product_id', 'product_category', 'product_name',
               'product_price', 'scrapy_datetime','style_id', 'color_name', 'color_id', 'fit', 
               'cotton', 'polyester', 'spandex', 'elastomultiester',
               'shell_cotton', 'shell_spandex', 'shell_polyester',
               'shell_elastomultiester', 'pocket_lining_cotton',
               'pocket_lining_spandex', 'pocket_lining_polyester',
               'pocket_lining_elastomultiester']]
    return data



def data_insert(data):
    ## Data Insert

    # Create database if not exist

    try:
        query_showroom_schema = '''
            CREATE TABLE table_products (
                product_id                       INTEGER,
                product_category                 TEXT,
                product_name                     TEXT,
                product_price                    REAL,
                scrapy_datetime                  TEXT,
                style_id                         TEXT,
                color_name                       TEXT,
                color_id                         TEXT,
                fit                              TEXT,
                size_number                      REAL,
                size_model                       TEXT,
                cotton                           REAL, 
                polyester                        REAL,
                spandex                          REAL,
                elastomultiester                 REAL,
                shell_cotton                     REAL,
                shell_spandex                    REAL,
                shell_polyester                  REAL,
                shell_elastomultiester           REAL,
                pocket_lining_cotton             REAL,
                pocket_lining_spandex            REAL,
                pocket_lining_polyester          REAL,
                pocket_lining_elastomultiester   REAL
            )
        '''

        # connect to dataset

        conn = sqlite3.connect('hm_db.sqlite')
        cursor = conn.execute(query_showroom_schema)
        conn.commit()
        conn.close()
    except:
        pass

    # Connetc to the Database
    conn = create_engine( 'sqlite:///hm_db.sqlite', echo=False )

    # Append data in Database
    data.to_sql('table_products',con = conn, if_exists='append', index=False)


if __name__ == '__main__':
    
    # logging
    if not os.path.exists( 'Logs' ):
        os.makedirs( 'Logs' )

    logging.basicConfig( 
        filename = 'Logs/webscraping_hm.txt',
        format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
        datefmt = '%Y-%m-%d %H:%M:%S',
        level=logging.DEBUG )

    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logger = logging.getLogger('webscraping_hm')
    
    # parameters and constants
    
    url = 'https://www2.hm.com/en_us/men/products/jeans.html?sort=stock&image-size=small&image=model&offset=0&page-size=108'
    headers = {
        'User-Agent':
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'
    }
    
    # data collection
    data = data_collection(url,headers)
    logger.info('data collect done')
    
    # data collection by product
    data_products = data_collection_by_product(data)
    logger.info('data collection by product done')
    
    # data cleaning
    data_cleaned = data_cleaning(data_products)
    logger.info('data product cleaned done')
    
    # data insertion
    data_insert(data_cleaned)
    logger.info('data insertion done')
    