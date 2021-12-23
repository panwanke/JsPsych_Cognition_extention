
import pandas as pd
import numpy as np
import os
import glob
import yaml
import time
from datetime import timedelta
import re
import pprint
from collections import Counter
from selenium import webdriver
from selenium.webdriver.support.ui import Select
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
      aa = timedelta(seconds = round(rt))
    elif unit == "milisecond":
      aa = timedelta(seconds = round(rt/1000))
    return aa

class jsdata():
  
  def __init__(self):
    self.name = "js_raw_data"
    self.__logurl = 'http://www.cognition.run/login'
    self.sub_id = "run_id"
    
  def load_data(self,*exp_block:list,dl_path:str="raw_data",block_id = "screen_id",custom_vars:list=[
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
    
    df = pd.read_csv(glob.glob("%s/*" %dl_path)[0],low_memory=False)
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
      tmp[sub_id] = int(i)
      tmp['condition'] = j['condition'].iloc[0]
      for k in custom_vars:
        aa = pd.unique(j[k])
        if len(aa) > 1:
          continue
        tmp[k]= aa[0]
      p_df = p_df.append(tmp,ignore_index=True)
    p_df.set_index(sub_id,inplace=True)
    self.p_df = p_df

    # saving behavior data  
    if not os.path.exists("R"):
      os.makedirs("R")
    save_df.to_csv("R/beha_data.csv")
    p_df.to_csv("R/person_data.csv")

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
          wt[sub_id] = [int(rid)]
          wt.set_index(sub_id,inplace=True)
          sub_df = pd.concat([sub_df,wt])
        if qstn_df.empty:
          qstn_df = sub_df
        else:
          qstn_df = pd.merge(qstn_df,sub_df,on=sub_id)
      
      qstn_df.to_csv("R/qst_score.csv")
      self._qst_data = qstn_df
      self._beha_data = save_df
      return save_df,qstn_df

    self._beha_data = save_df
    return save_df

  def log_web(self,dl_path="raw_data",**other_config):
    """logging in cognition web"""

    conf = read_conf()

    fp = webdriver.ChromeOptions()
    if not os.path.exists(dl_path):
      os.mkdir(dl_path)
    if "binary_path" in other_config:
      fp.binary_location = other_config["binary_path"]
    prefs={'download.default_directory':dl_path}
    fp.add_experimental_option('prefs',prefs)
    try:
      driver = webdriver.Chrome(options=fp)
    except:
      print("please install right browser driver")

    driver.get(self.__logurl)
    wait = WebDriverWait(driver,5)

    login1 = driver.find_element_by_id("emailInput")
    login2 = driver.find_element_by_id("passwordInput")
    login3 = driver.find_element_by_css_selector("button")

    login1.send_keys(conf["cog_account"])
    login2.send_keys(conf["cog_keys"])
    login3.click()
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, '.card-title')))

    return driver

  def dl_data(self,task:str,dl_path = "raw_data",**kwargs):
    """ download data from cognition
      @param: task: is a task number in your cogitntion
      @param: dl_path: is the relative instead of absolute path where the data will be saved
      note: you need to provide your account number and Keys
    """
    if not task:
      return print("please provide a task number")
    url2 = 'http://www.cognition.run/tasks/' + task

    driver = self.log_web(dl_path=dl_path,**kwargs)

    driver.get(url2)
    hrf_dl = driver.find_element_by_partial_link_text('Download')
    hrf_dl.click()

    finished_run = driver.find_element_by_xpath("//div[@class='form-group']/select[@name='filter']")
    s = Select(finished_run)
    s.select_by_value("finished")

    bt_dl = driver.find_element_by_xpath("//form/button[@type='submit']")
    files = glob.glob('%s\\*' %dl_path)
    if files:
      print("deleting already exists data", files)
      [os.remove(file) for file in files]
    bt_dl.click()

  def wait_finish(self,task:str,num:int=30,fz:int=300,**kwargs):
    """wait for stopping collecting data 
        while the number of participants beyong our goal
      @param task: is a task number in your cogitntion
      @param num: is the number of participants you want to collect
      @param fz: is the frequency of refresh browser. Unit is second
    """

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
  def __init__(self):
    super().__init__()
    self.col_pay = "p_pay"

  def split_within_vars(self,name="trials"):
    """convert the long form repeated vars to wide form data
    """

    import pandas as pd
    sub_id = self.sub_id
    df = self.df
    df = df.loc[df[self.block_id]!='prac_trials',:]
    names = pd.unique(df.loc[df[self.block_id].str.contains(name),self.block_id]) 
    #dfs = {}
    for name in names:
      df_ft = df[df[self.block_id]=="f_trials"].copy()
      df_tt = df[df[self.block_id]=="test_trials"].copy()

    aa = df_ft.reset_index().copy()
    for _,j in aa.groupby(sub_id):
      aa.loc[j.index.tolist()[0:40],"f_stage"] = 0
      aa.loc[j.index.tolist()[40:80],"f_stage"] = 1
    
    wide_df = pd.concat(
      [df_ft.set_index([sub_id,"s_number"]),
      df_tt.set_index([sub_id,"s_number"])],join='inner',axis=1,
      keys=['ft', 'tt'])

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
    temp = temp.join(p_df)
    temp.set_index(sub_id,inplace=True)

    # merge behavior data with questionnaire data
    if 'qn' in vars(self):
      qsdf = self._qst_data
      qsdf2 = qsdf.loc[:,qsdf.columns.str.contains("_rt")]
      merge_df = qsdf2.join(temp,how="inner")
    else:
      merge_df = temp
    
    # saving data
    f_path = os.getcwd() + "\\pay\\"
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

  def isPay(self,formula,cover=False):
    """judge whether the participants is should be paid or not"""
    
    f_path = 'pay\\pay.csv'
    nf_path = 'pay\\pay_withP.csv'
    dbfile = "pay\\pay_db.csv"
    col_pay = self.col_pay

    if "ct" in vars(self):
      df = self.ct
    elif os.path.isfile(f_path):
      df = pd.read_csv(f_path)
      df.set_index(self.sub_id,inplace=True)
    else:
      df = self.get_cost_time()
    df["problems"] = 100
    
    # check whther the payment account is duplicated
    if os.path.isfile(dbfile):
      db_df = pd.read_csv(dbfile,index_col=0)
      db_df.set_index(self.sub_id,inplace=True)
      o_accounts = list(pd.unique(db_df[col_pay]))
    n_accounts = list(pd.unique(df[col_pay]))
    accounts = o_accounts + n_accounts
    dup_accounts = {key:value for key,value in dict(Counter(accounts)).items() if value > 1}
    if len(dup_accounts) > 0:
      pprint("duplicated payment accounts:",dup_accounts)
      dup_df = df[df[col_pay].isin(list(dup_accounts.keys()))]
      self.dup_df = dup_df
      df.loc[df[col_pay].isin(list(dup_accounts.keys())),"problems"] -= 100
    # df.loc[df.duplicated(subset=col_pay,keep=False),"problems"] -= 100

    # sort the dataframe by the formula rules
    formula = [i.strip() for i in formula.split("\n") if i]
    col_ids = df.columns
    for fml in formula:
      elmts = re.split(r"[<=>]+",fml)
      symbol = re.search(r"([<=>]+)",fml).group(0)
      col_id = [i for i in col_ids if elmts[0] in i]
      
      if "<" in symbol:
        df[df[col_id] < float(elmts[1])]["problems"] -= 1
      elif ">" in symbol:
        df[df[col_id] < float(elmts[1])]["problems"] -= 1
    
    self.ct = df
    if cover:
      df.to_csv(nf_path)
      self.save2paydb()
    return df

  def save2paydb(self):
    """save the payaccount data to the database"""

    dbfile = "pay\\pay_db.csv"
    f_path = 'pay\\pay_withP.csv'
    col_pay = self.col_pay
    # get the data db
    if os.path.isfile(dbfile):
      o_df = pd.read_csv(dbfile)
    else:
      o_df = pd.DataFrame()

    # get the new data with pay
    if "ct" in vars(self):
      n_df = self.ct
    elif os.path.isfile(f_path):
      n_df = pd.read_csv(f_path).set_index(self.sub_id,inplace=True)
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

  

