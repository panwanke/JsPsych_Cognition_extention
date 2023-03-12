
import pandas as pd
import numpy as np
import os
import glob
from pandas._libs.missing import NA
import yaml
import time
import datetime
import json
import re
import pprint
from collections import Counter
from selenium import webdriver
from selenium.webdriver.support.select import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

def read_conf():
    with open("conf.yaml", 'r') as stream:
        try:
            conf = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return conf

def ms2seconds(rt,unit="second"):
    if not rt:
      return 0
    if unit == "second":  
      aa = time.strftime("%H:%M:%S", time.gmtime(rt))
    elif unit == "milisecond":
      aa = time.strftime("%H:%M:%S", time.gmtime(rt/1000))
    return aa

def time2sec(t):
  var = ("hours","minutes","seconds")
  return int(datetime.timedelta(**{k:int(v) for k,v in zip(var,t.strip().split(":"))}).total_seconds())

class jsdata():
  
  def __init__(self,task:str,dl_path:str):
    self.name = "js_raw_data"
    self.__logurl = 'http://www.cognition.run/login'
    self.sub_id = "run_id"
    self.task = task
    self.dl_path = dl_path
  
  def isElementExist(self,by,el,*driver):
    flag=True
    if driver:
      driver = driver[0]
    else:
      driver=self.driver
    try:
        element = driver.find_element(by,el)
        return element
    except:
        flag=False
        return flag

  def load_data(self,*exp_block:list,block_id = "screen_id",custom_vars:list=[
    'p_name','p_age','p_gender','p_pay','p_school',
    'correct','s_number','acc'],**other):
    """ This function is called to transform and save jspsychdata
        @param: dl_path is data path
        @param: exp_block is a list of blocks you want to collect (e.g. ['phase1','phase2',-'fixation'])
        @param: qstn is questionnaires block name, if qstn exists, the data will be parsed
        @param: block_id is trials block name
    """

    self.block_id = [block_id]
    self.custom_vars = custom_vars
    sub_id = self.sub_id
    self.origin_vars = [sub_id,'condition','trial_index','rt','response','stimulus']
    dl_path = self.dl_path

    files = glob.glob("%s/*" %dl_path)
    df = pd.DataFrame()
    for file in files:
      if df.empty:
        df = pd.read_csv(file,low_memory=False,index_col=False)
      else:
        df = df.append(pd.read_csv(file,index_col=False))
    self.df = df
    all_ids = pd.unique(df[block_id])

    # selecting participants
    if "par_id" in other:
      df = df[df[sub_id].isin(other['par_id'])]
    print("loading subjects: ",len(pd.unique(df.run_id)))

    # selecting usefull columns
    df = df.loc[:,self.origin_vars+self.custom_vars+self.block_id].copy()
    # deleting useless columns like fixation and instructions
    if exp_block:
      del_cols = [j for i in exp_block[0] if "-!" in i for j in all_ids if i[2:] in j]
      save_cols = [j for i in exp_block[0] if not i in del_cols for j in all_ids if i in j]
      save_df = pd.DataFrame()
      if save_cols:
        for i in save_cols:
          tmp = df[df[block_id]==i].copy()
          save_df = pd.concat([save_df,tmp],axis=0)
      if del_cols:
        for i in del_cols:
          save_df = save_df[save_df[block_id]!=i].copy()
    else:
      save_df = df.copy()
    self.bn = list(pd.unique(save_df[block_id]))
    save_df["rt"] = save_df["rt"].astype(float)/1000 # pd.to_numeric(save_df["rt"],errors='coerce')
    save_df.set_index(sub_id,inplace=True)

    # saving personal basic information
    p_df = pd.DataFrame()
    for i,j in df.groupby(sub_id):
      tmp = {}
      tmp[sub_id] = i
      tmp['condition'] = j['condition'].iloc[0]
      for k in custom_vars:
        aa = pd.unique(j[k])
        if len(aa) > 1:
          continue
        tmp[k]= aa[0]
      p_df = p_df.append(tmp,ignore_index=True)
    p_df = p_df.convert_dtypes()
    p_df.set_index(sub_id,inplace=True)
    self.p_df = p_df

    # saving behavior data  
    if not os.path.exists("R"):
      os.makedirs("R")
    save_df = save_df.convert_dtypes().copy()
    save_df.to_csv("R/beha_data.csv",encoding="utf-8")
    p_df.to_csv("R/person_data.csv",encoding="utf-8")

    # if questionnaires exists, parse them
    if "qstn" in other:
      bid = other["qstn"]
      block_ids = [i for j in bid for i in all_ids if j in i]
      self.qn = block_ids
      qstn_df = pd.DataFrame()
      for bid in block_ids:
        qs_df = df.loc[df[block_id]==bid].copy()

        # parsing data for each subjs
        sub_df = pd.DataFrame()
        for rid,data in qs_df.groupby(sub_id):
          w = pd.DataFrame([eval(q) for q in data["response"].values]) # 提取每个分量表
          w.columns = [str(bid)[0:3] + "_" + m for m in w.columns.values] # 给题目命名
          t = (pd.DataFrame(data["rt"],dtype="float"))/1000 # 加入反应时
          t.columns = str(bid)[0:3] + "_" + t.columns  
          wt = w.join(t.reset_index(drop=True))
          wt[sub_id] = rid
          wt.set_index(sub_id,inplace=True)
          sub_df = pd.concat([sub_df,wt])
        if qstn_df.empty:
          qstn_df = sub_df
        else:
          qstn_df = pd.merge(qstn_df,sub_df,on=sub_id)
      
      qstn_df = qstn_df.convert_dtypes().copy()
      qstn_df.to_csv("R/qst_score.csv",encoding="utf-8")
      self._qst_data = qstn_df
      self._beha_data = save_df
      return save_df,qstn_df

    self._beha_data = save_df
    return save_df

  def log_web(self,**other_config):
    """logging in cognition web"""

    conf = read_conf()
    dl_path = self.dl_path

    fp = webdriver.ChromeOptions()
    if not os.path.exists(dl_path):
      os.mkdir(dl_path)
    if "binary_path" in other_config:
      fp.binary_location = other_config["binary_path"]
    dl_path = os.path.join(os.getcwd(),dl_path)
    prefs={'download.default_directory':dl_path}
    fp.add_experimental_option('prefs',prefs)
    try:
      driver = webdriver.Chrome(options=fp)
    except:
      print("please install right browser driver")
      return

    driver.get(self.__logurl)
    wait = WebDriverWait(driver,5)

    login1 = driver.find_element_by_id("emailInput")
    login2 = driver.find_element_by_id("passwordInput")
    login3 = driver.find_element_by_css_selector("button")

    login1.send_keys(conf["cog_account"])
    login2.send_keys(conf["cog_keys"])
    login3.click()
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.card-title')))

    self.driver = driver
    return driver

  def dl_data(self,**kwargs):
    """ download data from cognition
      @param: task: is a task number in your cogitntion
      @param: dl_path: is the relative instead of absolute path where the data will be saved
      note: you need to provide your account number and Keys
    """
    task = self.task
    dl_path = self.dl_path
    url2 = 'http://www.cognition.run/tasks/' + str(task)

    if "driver" in vars(self):
      driver = self.driver
    driver = self.log_web(**kwargs)
    if not driver:
      return print("wrong browser driver")
    
    driver.get(url2)
    hrf_dl = driver.find_element(By.PARTIAL_LINK_TEXT,'Download')
    hrf_dl.click()

    finished_run = driver.find_element(By.XPATH,"//div[@class='form-group']/select[@name='filter']")
    s = Select(finished_run)
    s.select_by_value("finished")

    bt_dl = driver.find_element(By.XPATH,"//form/button[@type='submit']")
    files = glob.glob('%s\\*' %dl_path)
    if files:
      print("deleting already exists data", files)
      [os.remove(file) for file in files]
    bt_dl.click()

  def wait_finish(self,num:int=30,fz:int=300,**kwargs):
    """wait for stopping collecting data 
        while the number of participants beyong our goal
      @param task: is a task number in your cogitntion
      @param num: is the number of participants you want to collect
      @param fz: is the frequency of refresh browser. Unit is second
    """
    task = self.task
    url2 = 'http://www.cognition.run/tasks/' + task + '/edit'

    n = 0
    driver = self.log_web(**kwargs)
    while n<num:
      driver.get(self.__logurl)
      try:
        if driver.find_element_by_xpath("//h1[contains(text(),'Sign in')]").is_displayed():
          driver.close()
          driver = self.log_web()
          print ("掉线了，尝试重新连接")
      except:
        print("没有掉线")
      else:
        print("重连成功")
      finally:
        p_num = driver.find_element_by_xpath("//a[contains(@href,'%s')]/../../..//h5" %task)
        n = int(p_num.text)
        print("被试数：",n)
        #driver.refresh()
        time.sleep(fz)
    
    driver.get(url2)
    time.sleep(1)
    bt = driver.find_element_by_xpath("//input[@name='is_enabled']")
    if bt.is_selected():
      bt.click()
    driver.find_element_by_xpath("//button[@type='submit']").click()

    self.dl_data(task,**kwargs)

    driver.close()
    print("collection have been done")

  def get_finish_list(self,**kwargs):
    """get finished data"""
    task = self.task

    url = 'http://www.cognition.run/tasks/' + task
    driver = self.log_web(**kwargs)
    driver.get(url)

    next_page = True
    res_list = []
    while next_page:
      tbody = driver.find_element(By.XPATH,"//tbody")
      trs = tbody.find_elements(By.TAG_NAME,"tr")
      res = [int(tr.find_element(By.XPATH,".//td[1]").text[1:]) for tr in trs if tr.find_element(By.XPATH,".//td/span").text == "Finished"]
      res_list += res
      next = self.isElementExist(By.CSS_SELECTOR,'a[rel="next"]',driver)
      if next==False:
        next_page = False
      else:
        next.click()
        time.sleep(0.1)

    return res_list    

class matchjd(jsdata):
  """matchjd means the jsdata have questionnaires items in which be used 
    in the posterior behavior task, we thus need to match these items between
    qestionnaire items and behavior trials.

    operation principle: 
      download data from cognition in "raw_data" folder, 
      then transform the data to pay account information in "pay" folder,
      The pay.csv is new data, and the pay_withP.csv is old.
      Every time you download new data, the pay_withP.csv will be coverd.
      After that, you can integrate the pay_withP.csv into pay database ("pay_db.csv").
  """
  def __init__(self,*args,**kwargs):
    super().__init__(*args,**kwargs)
    self.col_pay = "p_pay"

  def split_within_vars(self,
                        stim:str="s_number",
                        within_vars:str="f_stage",
                        DVs = ["rt","response"],
                        **kwargs):
    """convert the long form repeated vars to wide form data
    """

    sub_id = self.sub_id
    self.stim = stim
    df = self._beha_data
    block_id = self.block_id[0]
    bnames = pd.unique(df[block_id]) # get all block names

    if "exclude" in kwargs:
      ex_list = kwargs["exclude"]
      ex_list = [i for i in bnames for j in ex_list if j in i]
      df = df.query("%s not in %s" %(block_id,tuple(ex_list)))
      bnames = [i for i in bnames if i not in ex_list]
    
    wt_list = [i for i in bnames if within_vars in i]
    df_wt = df.query("%s in %s" %(block_id,tuple(wt_list)))[[block_id,stim] + DVs].copy()
    df_wt[within_vars] = df_wt[block_id].apply(lambda x: re.sub(within_vars, '', x)).copy()
    df_wt.drop(block_id,axis=1,inplace=True)
    DVs_wt = {i:"%s_%s" %(within_vars,i) for i in DVs}
    df_wt.rename(columns=DVs_wt,inplace=True)

    bnames = [i for i in bnames if i not in wt_list] 
    # df = df[stim].astype(int).copy()
    # df_rest = df[df[stim]==None][[block_id] + DVs].copy()
    df_rest = df[df[stim]=='"'][[block_id] + DVs].copy()
    rest_DVs = list(pd.unique(df_rest[block_id]))
    tmp = pd.DataFrame()
    for i in rest_DVs:
      tmp2 = df_rest[df_rest[block_id]==i].copy()
      for j in DVs:
        tmp2["%s_%s" %(i,j)] = tmp2[tmp2[block_id]==i][j].copy()
      tmp2.drop(DVs+[block_id],axis=1,inplace=True)
      if tmp.empty:
        tmp = tmp2
      else:
        tmp = tmp.join(tmp2)
    df_rest = tmp.copy()

    bnames = [i for i in bnames if i not in rest_DVs]
    old_df = df.query("%s in %s" %(block_id,tuple(bnames))).copy()
    new_df = pd.merge(old_df,df_rest,right_index=True,left_index=True,how="left")

    wide_df = pd.merge(new_df,df_wt,on=[stim,sub_id],how="outer")
    wide_df.to_csv("R/beha_data.csv",encoding="utf-8")
    self.wdf = wide_df
    
    return wide_df

  def get_cost_time(self):
    """compulate the cost time for each participants"""

    if self.df.empty:
      return print("please load data first")
    sub_id = self.sub_id
    block_id = self.block_id
    # bn = self.bn
    bhdf = self._beha_data
    p_df = self.p_df

    # deal with behavior data
    temp = pd.DataFrame()
    for sid,data in bhdf.groupby(sub_id):
      temp2 = {}
      temp2[sub_id] = sid
      for bid,data2 in data.groupby(block_id):
        temp2[bid] = ms2seconds(data2.rt.sum())
        if data2.shape[1] > 4:
          temp2[bid+"_rt_max"] = round(np.max(data2.rt),5)
          temp2[bid+"_rt_min"] = round(np.min(data2.rt),5)
          temp2[bid+"_rt_mean"] = round(data2.rt.mean(),5)
          temp2[bid+"_res_mean"] = round(
            pd.to_numeric(data2.response,errors="coerce").mean(),5)
        else:
          temp2[bid+"_rt"] = round(data2.rt,5)
      temp=temp.append(temp2,ignore_index=True)
    temp = temp.convert_dtypes()
    temp.set_index(sub_id,inplace=True)
    temp = pd.merge(temp,p_df,on=sub_id,how="left")
    
    # merge behavior data with questionnaire data
    if 'qn' in vars(self):
      qsdf = self._qst_data
      qsdf2 = qsdf.loc[:,qsdf.columns.str.contains("_rt")]
      qsdf2 = qsdf2.applymap(lambda x: ms2seconds(x)).copy()
      merge_df = qsdf2.join(temp,how="inner")
    else:
      merge_df = temp
    
    # saving data
    f_path = os.getcwd() + "\\pay\\"
    if not os.path.exists(f_path):
      os.makedirs(f_path)
    f_loc = f_path +'pay.csv'

    if os.path.isfile(f_loc):
      print('file exists, updating will be executed')
      f = pd.read_csv(f_loc)
      f.set_index(sub_id,inplace=True)
      # deleting existing run_id
      rid = [i for i,_ in f.groupby(sub_id)]
      merge_df = merge_df[~merge_df.index.isin(rid)].copy() 
      if merge_df.empty:
        print("no new data transform for cost time information")
        return
      f = pd.concat([merge_df,f])
      f.to_csv(f_loc)   
    elif os.path.exists(f_path):
      if not os.path.exists(f_path):
        os.makedirs(f_path)
      merge_df.to_csv(f_loc)
    self.ct = merge_df # ct means cost time

    return merge_df

  def isPay(self,formula,cover=True):
    """judge whether the participants is should be paid or not"""
    
    f_path = 'pay\\pay.csv'
    nf_path = 'pay\\pay_withP.csv'
    dbfile = "pay\\pay_db.csv"
    col_pay = self.col_pay
    sub_id = self.sub_id

    if not os.path.exists("pay"):
      os.makedirs("pay")
    if "ct" in vars(self):
      df = self.ct
    elif os.path.isfile(f_path):
      df = pd.read_csv(f_path)
      df.set_index(sub_id,inplace=True)
    else:
      df = self.get_cost_time()
    prob = "problems"
    df[prob] = 100
    df = df.convert_dtypes()
    
    # check whther the payment account is duplicated
    accounts = list(pd.unique(df[col_pay]))
    if os.path.isfile(dbfile):
      db_df = pd.read_csv(dbfile,index_col=0)
      db_df.set_index(sub_id,inplace=True)
      o_accounts = list(pd.unique(db_df[col_pay]))
      accounts = o_accounts + accounts
    dup_accounts = {key:value for key,value in dict(Counter(accounts)).items() if value > 1}
    if len(dup_accounts) > 0:
      print("duplicated payment accounts:")
      pprint.pprint(json.dumps(dup_accounts))
      dup_df = df[df[col_pay].isin(list(dup_accounts.keys()))]
      self.dup_df = dup_df
      df.loc[df[col_pay].isin(list(dup_accounts.keys())),prob] -= 90
    # df.loc[df.duplicated(subset=col_pay,keep=False),prob] -= 100

    # sort the dataframe by the formula rules
    formula = [i.strip() for i in formula.split("\n") if i]
    col_ids = df.columns
    for fml in formula:
      elmts = re.split(r"[<=>]+",fml)
      symbol = re.search(r"([<=>]+)",fml).group(0)
      col_id = [i for i in col_ids if elmts[0] in i][0]
      
      if str(df[col_id].dtype) == "string":
        diff_sec = df[col_id].apply(lambda x: time2sec(x))
      else:
        diff_sec = df[col_id].copy()
      if "<" in symbol:
        df.loc[diff_sec < float(elmts[1]),prob] -= 1
      elif ">" in symbol:
        df.loc[diff_sec > float(elmts[1]),prob] -= 1
    
    self.ct = df
    if cover:
      if os.path.isfile(nf_path):
        tmp = pd.read_csv(nf_path,index_col=sub_id)
        tmp2 = pd.unique(tmp.index)
        df = df[~df.index.isin(tmp2)]
        df.append(tmp).to_csv(nf_path,index=True)
      else:
        df.to_csv(nf_path,index=True)
    return df

  def save2paydb(self):
    """save the payaccount data to the database"""

    dbfile = "pay\\pay_db.csv"
    f_path = 'pay\\pay_withP.csv'
    col_pay = self.col_pay
    sub_id = self.sub_id
    # get the data db
    if os.path.isfile(dbfile):
      o_df = pd.read_csv(dbfile)
    else:
      o_df = pd.DataFrame()

    # get the new data with pay
    if "ct" in vars(self):
      n_df = self.ct
    elif os.path.isfile(f_path):
      n_df = pd.read_csv(f_path).set_index(sub_id)
    else:
      print("no pay data, pleasing run isPay before")
      return
    
    # merge the data
    if o_df.empty:
      db_df = n_df
    else:
      pay_accounts = list(pd.unique(o_df[col_pay]))
      tmp = n_df[~n_df[col_pay].isin(pay_accounts)]
      print("adding new pay accounts:",tmp.shape[0])
      db_df = pd.concat([o_df,tmp])
    db_df.to_csv(dbfile)

    return db_df

  def describe(self,key_map=None,save=True):
    """describe the data"""

    if self.wdf.empty:
      print("no data to describe")
      return
    df = self.wdf
    df.reset_index(inplace=True)
    sub_id = self.sub_id
    s_number = self.stim
    res = "response"
    DVs = [i for i in df.columns for j in [res,"rt","acc"] if j in i]
    if key_map:
      df[res] = df[res].map(key_map)
    df = df[DVs+[sub_id,s_number]]
    df = df.astype(float).copy()

    tmp = pd.DataFrame()
    for _,data in df.groupby(by=sub_id,as_index=False):
      tmp2 = data.mean()
      tmp3 = data.dropna().copy()
      tmp2["res_max"] = "%d,%d" %(tmp3[res].sum(),tmp3[res].shape[0])
      tmp = tmp.append(tmp2,ignore_index=True)
    df_p = tmp.convert_dtypes().copy()
    df_p.set_index(sub_id,inplace=True)

    tmp = pd.DataFrame()
    for _,data in df.groupby(by=s_number,as_index=False):
      tmp2 = data.mean()
      tmp3 = data.dropna().copy()
      tmp2["res_max"] = "%d,%d" %(tmp3[res].sum(),tmp3[res].shape[0])
      tmp = tmp.append(tmp2,ignore_index=True)
    df_stim = tmp.convert_dtypes().copy()
    df_stim = df_stim.loc[:,~df_stim.columns.str.contains("trial_")]
    df_stim.set_index(s_number,inplace=True)

    if save:
      df_stim.to_csv("R/describe_stim.csv")
      df_p.to_csv("R/describe_individual.csv")

    return df_p.sort_values(["rt","response"],ascending=False),df_stim.sort_values(["rt","response"],ascending=False)

