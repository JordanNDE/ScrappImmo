from flask import Flask, request, redirect, url_for, flash, jsonify
import numpy as np
import pickle as p
import json
import pandas as pd
from unidecode import unidecode
import re
import os

from pathlib import Path
parent_path = Path(__file__).parents[0]
os.chdir(parent_path)


path_to_data = 'data/'



#FONCTIONS

# def transform_annonces(df):
    # #Attention s'assurer que les noms des colonnes vont pas changer 
    # #l_keep = ['_id', 'adId','city', 'description', 'origin', 'postalCode', 'price', 'publicationDate','rooms','square','title','type','url']
	# l_keep = ['_id']
	# l_final = list(set(df.columns.tolist())&set(l_keep))
	# df_res = df[l_final]
	# #df_res.rename(columns={'description':'Description','city':'ville'}, inplace=True)
	# #df_res['ville'] = [str(j).strip().lower() for j in list(df_res['ville'].values)] 
	# df_res['score'] = 99
	# return(df_res)

def transform_annonces(df):
    #Attention s'assurer que les noms des colonnes vont pas changer 
    l_keep = ['_id', 'adId','city', 'description', 'origin', 'postalCode', 'price', 'publicationDate','rooms','square','title','type','url']
    l_final = list(set(df.columns.tolist())&set(l_keep))
    
    df_res = df[l_final]
    df_res.rename(columns={'description':'Description','city':'ville'}, inplace=True)
    df_res['ville'] = [str(j).strip().lower() for j in list(df_res['ville'].values)] 
    
    return(df_res)
	
	
""" Prend la matrice des quartiers et en fait un dictionnaire """
#Afin de rationaliser les quartiers 
def get_df_make_dict(df,l_ville):
    df['ville'] = [str(j).strip() for j in list(df['ville'].values)] 
    
    df = df[df['ville'].isin(l_ville)]
    dico_final = {}
    
    for row in df.index.tolist():
        des_tiek = df.loc[row,'quartier_source'].split(',')
        #print(des_tiek)
        d_tiek = {}
        for tiek in des_tiek:
            d_tiek[tiek] = df.loc[row,'quartier_cible']
        dico_final.update(d_tiek)
#         df['list_dict'] = df[]
#         for k in df.loc[row,'quartier_source']
    
    return(dico_final)	
	
def convert_to_string_cols(df,l_cols):
    for col in l_cols:
        df[col] = [str(j) for j in list(df[col].values)] 
    return(df)	
	
def transform_description(des):
    #élimination des accents + tolower() + élimination des espaces de début et fin de phrase
    des_inter = unidecode(des.lower().strip())
    #print(des_inter)
    
    #remplace les '\n' et '\r' (sauts de lignes) par un espace
    des_inter_1 = re.sub(r'[\n\r]', ' ',des_inter)
    
    #Enlève les caractères spéciaux et fait une liste à partir du charactère ' ' 
    res = re.sub(r'[^\w\s]',' ',des_inter_1).split(" ")
    
    l_res = [k for k in res if (len(k)>2)]
    return(l_res)
	
def fill_match_quartier(x,description):
    res = set(x.split(','))&set(transform_description(description))
    if(len(res)==0):
        res = ''
    else:
        res = list(res)[0]
    return(res)
	
	
""" Fonciton de matching qui à une description associe le quartier concerné !! """
def find_match(df_quartier,description,dico):
    #val = [str(k) for k in list(df_quartier['quartier_source'].apply(lambda x: fill_match_quartier(x,description))) if k != ''][0]
    #output = dico[val]
    try:
        val = [str(k) for k in list(df_quartier['quartier_source'].apply(lambda x: fill_match_quartier(x,description))) if k != ''][0]
        output = dico[val]
    except:
        print("aucun match")
        output = ''
    return(output)
	
	
def powerful_quartier_finder(table_de_match,table_description):
    #preparation de table_de_match
    df = table_de_match
    df.columns = [str(k).strip() for k in table_de_match.columns]
    df = convert_to_string_cols(df,df.columns)
    
    df['ville'] = [str(j).strip() for j in list(df['ville'].values)] 
    
    l_df = []
    #On sépare par ville car il y a des villes qui ont à peu près les mêmes nom de quartier, afin d'assurer une unicité pour nos 
    # clés de dictionnaire 
    for ville in table_description['ville'].unique().tolist():
        print(ville)
        temp_df = df[df['ville']==ville]
        temp_des = table_description[table_description['ville']==ville.strip()]
        dico = get_df_make_dict(temp_df,[ville])
        if(len(dico)==0):
            print("la ville {} n'est pas encore répertorié, elle fera l'objet de rajout incésemment sous peu".format(ville))
            pass
        else:
            temp_des['quartier_cible'] = temp_des['Description'].apply(lambda x: find_match(temp_df,x,dico))
            l_df.append(temp_des)
        #print("*"*20)
        
    df_final = pd.concat(l_df)  
    
    return(df_final)
	


def format_string(x):
	res = unidecode(str(x).strip().lower()) 
	return(res)


def clean_the_mess(df,l_col):
	df[l_col] = df[l_col].applymap(lambda x: format_string(x))
	return(df)
	
def custom_merge(res,magic_table):
    l_df = []
    for ville in res['ville'].unique().tolist():
        #print(ville)
        tp_mgic_tbl = magic_table[magic_table['ville']==ville]
        tp_res = res[res['ville']==ville]
        temp_df = pd.merge(tp_res,tp_mgic_tbl[list(set(tp_mgic_tbl.columns.tolist())-set(['ville','periode']))],how='left',on='quartier_cible')
        tp_id = str(tp_mgic_tbl['inseeCommune'].unique().tolist()[0])+'_nan'
        #print(tp_id)
        temp_df['default_median_price'] = list(tp_mgic_tbl.loc[tp_mgic_tbl['id']==tp_id,'prixM2Median'])[0]
        temp_df['prixM2Median'].fillna(temp_df['default_median_price'],inplace=True)
        l_df.append(temp_df)
    
    df_final = pd.concat(l_df)
    return(df_final)
	

def calculate_sp(df):   
    l_col_num = ['square','prixM2Median','price'] #'rooms','population','z_moyen','default_median_price',
    for col in l_col_num:
        df[col] = df[col].astype(dtype=np.float64)
	
    df['price_median'] = df['square']*df['prixM2Median']


    df_0 = df[df['type']=='rentals']
    df_1 = df[df['type']=='sales']
    
    #Car ce sont des rentals et ne sont pas concernés par ça 
    df_0['surpricing'] = 999
    
    #On crée l'attribut de surpricing
    df_1['surpricing'] = 100*((df_1['price']-df_1['price_median'])/(df_1['price_median']))
    df_1['surpricing'] = df_1['surpricing'].astype(dtype=np.int32)
    df_res = pd.concat([df_1,df_0])
    return(df_res.sort_values(by='surpricing'))
	
	
#MAIN 
app = Flask(__name__)
@app.route('/api', methods=['POST'])
def makecalc():
	#récupération de la table magique !
	path_magic_table = path_to_data+'magic_table'
	#path_magic_table = "C:/Users/jordan.ndetcho/Documents/Jordan/IMT_Livrables/Jo/Personal/scrap_immo/data/data_python/magic_table.xlsx"
	#magic_table_match = pd.read_excel(path_magic_table)
	magic_table_match = pd.read_pickle(path_magic_table)
	magic_table_match.columns = [str(k).strip() for k in magic_table_match.columns]
	
	
	data = request.get_json()
	df = pd.DataFrame(data)
	df_ = transform_annonces(df)
	
	#res = list(df_annonces['price'])[0]
	#res = df_.to_json(orient='records')
	
	
	#completion du prix associé 
	res = powerful_quartier_finder(magic_table_match,df_)
	res = clean_the_mess(res,res.columns.tolist())
	df_f = custom_merge(res,magic_table_match)
	df_ff = calculate_sp(df_f) 
	
	
	#retransforme les données en json - retour du 'post'
	res_v0 = df_ff.to_json(orient='records')
	res_v1 = jsonify(res_v0)
	#res_v1 = json.dumps(res, ensure_ascii=False)
	return(res_v1)
    
	


@app.route('/test', methods=['GET'])
def hey():

	return('hello')


if __name__ == '__main__':
	#app.config['JSON_AS_ASCII'] = False
	app.run(debug=True, host='127.0.0.1')