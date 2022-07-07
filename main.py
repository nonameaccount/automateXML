# import required libraries
# TODO: requirements.txt
import pandas as pd
import numpy as np
import mysql.connector
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import re
import os
from lxml import etree

def func_xml_create(df_xml_part):
    # Extract required section from extracted data dictionary into dataframe
    col_path_lst = re.findall('\(\s?(.+?)\s?\\)', df_xml_part.Path.unique()[0])
    col_unbnd_lst = np.unique(np.array(df_xml_part['Col_Unbound'].dropna().to_list())).tolist()
    col_data_lst = np.unique(np.array(df_xml_part['Col_Data'].dropna().to_list())).tolist()
    col_lst1 = col_path_lst + col_unbnd_lst + col_data_lst
    col_lst = np.unique(','.join(col_lst1).split(','))

    if any(col_lst):
        dct_master_data = dct_master[df_xml_part.Type.unique()[0]][col_lst].drop_duplicates().reset_index()

    if not any(col_lst):
        var_iter = 1
    else:
        var_iter = len(dct_master_data.index)

    while var_iter > 0:
        for index1, row1 in df_xml_part.iterrows(): ### read xml structure
            path = df_xml_part['Path'][index1] ### path of the node
            fold = df_xml_part['Col_tag'][index1] ### node name
            fold_data = df_xml_part['Col_Data'][index1] ### data to be mapped

            if str(df_xml_part.Col_Unbound[index1]) != 'nan':  ### check if xml row is unbound
                unbnd_flg = 'Y'
                var_unbnd = len(dct_master_data[df_xml_part.Col_Unbound[index1].split(',')].drop_duplicates())
                ar_unbnd_val1 = dct_master_data[df_xml_part.Col_Unbound[index1].split(',')].apply(lambda x: ''.join(x), axis = 1).to_numpy()
                ar_unbnd_val = ar_unbnd_val1[var_iter - 1]
            else:
                var_unbnd = 1
                unbnd_flg = 'N'

            regexp = re.compile(r'/')
            regexp1 = re.compile(r'\(')

            if (var_unbnd != 1) or (unbnd_flg == 'Y'):
                fold_nm = fold + '-' + re.sub(r'[^\w]','',str(ar_unbnd_val))
            else:
                fold_nm = fold

            path_nm = path

            for i in re.findall('\(\s?(.+?)\s?\\)', path_nm):
                lst = i.split(',')
                path_nm = re.sub(r'[\s\+\.]', '', re.sub(f'\({i}\)', '-' + re.sub(r'[^\w]', '', dct_master_data[lst].apply(lambda x: ''.join(x), axis = 1)[var_iter - 1]), path_nm))

            if regexp.search(path_nm):
                for elemt in root.findall(path_nm):
                    child = ET.SubElement(elemt, fold_nm)
            else:
                for elemt in root.iter(path_nm):
                    child = ET.SubElement(elemt, fold_nm)

                var_unbnd = var_unbnd - 1

            if str(df_xml_part['Col_Data'][index1]) != 'nan':
                child.text = str(eval(f'dct_master_data.{fold_data}')[var_iter - 1]).strip()

        var_iter = var_iter - 1


# define variables
config = {
    'user':'xmlAccount',
    'password':'Passw0rd123#',
    'host':'192.168.1.250',
    'database':'automateXML',
    'raise_on_warnings': True
}
lst_pth = []
var_pth = ''
var_create = 0
df_xml_part = pd.DataFrame(None)

try:
    try:
        # connect to database
        conn = mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        print(err)

    try:
        ### EXTRACT
        # Unloading SQL data for XML
        dct_master = dict()
        df = pd.read_excel("config/XML_config.xlsx", sheet_name='QueryDetails')
        x = df.Type.unique().tolist()

        for index,rows in df.iterrows():
            col_alias = []
            var_sql = df['sql'][index]

            # Query executed and output stored in dictionary
            df_data = pd.read_sql(var_sql, conn)
            dct_master[df['Type'][index]] = df_data

        ### Extract XML Structure
        df_xml = pd.read_excel("config/XML_config.xlsx", sheet_name='xmlStructure').sort_values(by=['Row_seq'])

        for index, rows in df_xml.iterrows():
            if index == 0:
                name_space = {
                    "xmlns:nl":"urn:StandardAuditFile-Taxation-Financial:NO",
                    "xsi:schemaLocation":"urn:StandardAuditFile-Taxation-Financial:NO XML_Final.xsd",
                    "xmlns:xsi":"http://www.w3.org/2001/XMLSchema-instance"
                }
                root = ET.Element(df_xml[df_xml['Type'] == 'root']['Col_tag'][0], name_space)
            else:
                if (index != 1):
                    if ((var_pth != df_xml['Path'][index]) or (str(df_xml.Col_Unbound[index]) != 'nan')):
                        func_xml_create(df_xml_part)
                        df_xml_part = pd.DataFrame(None)

                var_pth = df_xml['Path'][index]
                df_xml_part = df_xml_part.append(df_xml[['Type', 'Path', 'Col_tag', 'Col_Unbound', 'Col_Data']][index:index + 1], ignore_index=True)
                #df_xml_part = df_xml_part.concat(df_xml[['Type', 'Path', 'Col_tag', 'Col_Unbound', 'Col_Data']][index:index + 1], ignore_index=True)

        ### Handling end of XML file
        func_xml_create(df_xml_part)
        df_xml_part = pd.DataFrame(None)

        ### Formatting of the final XML file
        regexp_no_data = re.compile(r'/>')
        regexp_no_data1 = re.compile(r'> <')

        with open("XML_Final1.xml", "w") as text_file:
            print(BeautifulSoup(ET.tostring(root), "xml").prettify(), file = text_file)

        # input file
        fin = open("XML_Final1.xml", "rt")

        # output file to write the result to
        fout = open("XML_Final2.xml", "wt")

        for l, line in enumerate(fin):
            if (l == 0):
                fout.write(line)
            else:
                if (l == 1):
                    line1 = re.sub(r'<', '<nl:', line)
                    line2 = re.sub(r'nl:/', '/nl:', line1)
                    fout.write((line2))
                else:
                    line1 = re.sub(r'<', '<nl:', line)
                    line2 = re.sub(r'nl:/', '/nl:', line1)
                    fout.write(re.sub(r'->', '>', re.sub(r'-[0-9a-zA-Z]+?>', '>', line2)))

        # close input and output files
        fin.close()
        fout.close()

        # discard strings which are entirely white space
        myparser = etree.XMLParser(remove_blank_text=True)

        abc = etree.parse('XML_Final2.xml', myparser)

        for elem in abc.iter('*'):
            if elem.text is not None:
                elem.text = elem.text.strip()
            if elem.tail is not None:
                elem.tail = elem.tail.strip()

        # write the xml file with pretty print and xml encoding
        abc.write('XML_Final3.xml', pretty_print=True, encoding="utf-8", xml_declaration=True)

        with open('XML_Final3.xml') as oldfile, open('XML_Final.xml', 'w') as newfile:
            for line in oldfile:
                if not regexp_no_data.search(line):
                    if not regexp_no_data1.search(line):
                        newfile.write(line)

        try:
            os.remove("XML_Final1.xml")
            os.remove("XML_Final2.xml")
            os.remove("XML_Final3.xml")
        except OSError:
            pass

    except Exception as e:
        print(str(e))

except Exception as e:
    print(str(e))
