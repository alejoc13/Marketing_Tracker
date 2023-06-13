import helper.loadData as ld
import helper.procesing as pr
import pandas as pd
from tqdm import tqdm
import numpy as np
tqdm.pandas()
def prepareData(token):
    filters = ld.chargeFilters()
    df = ld.uploadData()
    print('Generando CFNs Tratados:')
    df['Treated CFN'] = df.progress_apply(pr.treadCFNs,axis = 1)
    df = df.dropna(subset=['CFN'])
    df = pr.sp_trim(df)
    sp = ld.load_SPlan(token)
    sp = pr.sp_trim(sp)
    return df,sp,filters

def PrepareNotfound(df,filters):
    filterCFNs = list(filters['Treated'].unique())
    notFound = []
    for cfn in filterCFNs:
        if cfn in df['Treated CFN']:
            print('estoy aqui perrini')
            continue
        else:
            notFound.append(cfn)
    return notFound

def defineCriticalCFN(row,filterList,filters):
    a = row['Treated CFN']
    if a in filterList:
        try:
            aux = filters[filters['Treated'] == a]['Priority'].unique()
            b = aux[0]
        except:
            b = 'Not defined on List'
        return b
    else:
        return 'Not critical CFN'

def assignMPG(row,filterList,filters,assigner = 'MPG' ):
    a = row['Treated CFN']
    if a in filterList:
        try:
            aux = filters[filters['Treated'] == a][assigner].unique()
            b = aux[0]
        except:
            b = 'Not defined on List'
        return b
    else:
        return 'Not in Filters list'

def determinenotFound(df,FilterList):
    CnF = pd.DataFrame(columns = ['Treated CFN'])
    reference = list(df['Treated CFN'].unique())
    notFound = []
    for val in FilterList:
        if val in reference:
            pass
        else:
            notFound.append(val)
    
    CnF['Treated CFN'] = notFound
    return CnF

def searchOriginal(row,df2):
    a = row['Treated CFN']
    b = df2.loc[df2['Treated'] == a,'CFN'].unique()
    b= b[0]
    return b

def createPercentageMPG(df,filters):
    filters2 = filters.copy()
    print(filters.columns)
    filters2 = filters2[["MPG","Priority","CFN"]]
    filters2 = filters2.drop_duplicates(subset = ["CFN"])
    filters2['count']=1
    # expected_by_priority = filters2.groupby(by = ["MPG","Priority"],as_index = False).count()
    expected_by_priority = pd.pivot_table(filters2,index=['MPG'],columns=['Priority'],values='count',aggfunc=np.sum,fill_value=0)
    expected_by_priority = expected_by_priority.reset_index()
    renames = {name:f"expected for {name}"  for name in expected_by_priority.columns}
    del renames['MPG']
    expected_by_priority = expected_by_priority.rename(columns = renames)

    df = df[df['Critical?']!='Not in Filters list']
    df['count']=1
    df=df[['Country','MPG','Critical?','count']]
    stats=pd.pivot_table(df,index=['Country','MPG'],columns=['Critical?'],values='count',aggfunc=np.sum,fill_value=0)
    stats = stats.reset_index()
    stats = stats.merge(expected_by_priority, on = 'MPG')
    stats = stats.sort_values(by='Country')
    return stats

def filteringData(token):
    df,sp,filters = prepareData(token)
    listOU  = [ou.strip() for ou in filters['SubOU'].unique()]
    df = df[df['OU'].isin(listOU)]
    print('Buscando información en el Submission Plan:')
    df['Regulatory info'] = df.progress_apply( pr.searchSP,axis = 1,sp = sp)
    print('La información ya ha sido asignado a los Produtos')
    aux = list(filters['Treated'].unique())
    filterList = [val.strip() for val in aux]
    print('Asignando Prioridad a los Productos:')
    df['Critical?'] = df.progress_apply(defineCriticalCFN,axis = 1,filterList = filterList,filters =filters)
    print('Asignando MPG a los CFNs ')
    df['MPG'] = df.progress_apply(assignMPG,axis = 1,filterList = filterList,filters =filters)
    print('Asignando Global OU')
    df['Global OU'] = df.progress_apply(assignMPG,axis = 1,filterList = filterList,filters =filters,assigner = 'GLOBAL OU')
    print('Prioridad satisfactoriamente asignada')
    CnF = determinenotFound(df,filterList)
    df2 = filters.drop('SubOU',axis = 1)
    print('Generando CFNs no encontrados:')
    CnF['Original CFN'] = CnF.progress_apply(searchOriginal,axis=1,df2=df2)
    print('asignando MPG a los valores no encontrados')
    CnF['MPG'] = CnF.progress_apply(assignMPG,axis = 1,filterList = filterList,filters =filters)
    print('Asignando Global OU')
    CnF['Global OU'] = CnF.progress_apply(assignMPG,axis = 1,filterList = filterList,filters =filters,assigner = 'GLOBAL OU')
    print('Asignando Prioridades')
    CnF['Priority'] = CnF.progress_apply(defineCriticalCFN,axis = 1,filterList = filterList,filters =filters)
    stats = createPercentageMPG(df,filters)
    inCountry = pr.createInCountry(df,CnF)
    portfolio = pr.Createportfoliostatus(df,filters)
    byOU = pr.createSubOU(df)
    pr.create_excel(df,CnF,inCountry,portfolio,byOU,stats)