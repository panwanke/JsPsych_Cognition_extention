from js_module import matchjd
# init
binary_path=r"C:\Program Files\Google\Chrome\Application\chrome.exe"
task = '11638'   
dl_path = "raw_data" 
rs = matchjd(task,dl_path)

aa = rs.get_finish_list()

url = 'http://www.cognition.run/tasks/' + task
driver = rs.log_web()
driver.get(url)


# log in cognition
# rs.log_web()

# stop the task unitl the number of participants is enough
# rs.wait_finish(task,num=90,fz=5,binary_path=binary_path)

# download the data
# rs.dl_data(task,dl_path=dl_path,binary_path=binary_path)

# load the data as pd DataFrame
custom_vars=[
    'p_name','p_age','p_gender','p_pay',
    'p_school','correct','s_number','acc']
par_id = [
  48,49,46,41,38,26,25,23,
  3,9,12,18,20,
  1010,1025,1034
]
exp_block = ["trial","stage"]
df,_ = rs.load_data(
  exp_block,dl_path,qstn=['qs'],
  custom_vars=custom_vars,par_id=par_id)

# 查看基本信息
rs.get_cost_time() # 会储存一个 被试基本信息的数据库
# rs.ct
# print(rs.ct.head())

# 储存数据库 判断最小反应时；判断支付宝是否重复
formula = """
  lit_rt<40
  IUS_rt<40
  CTS_rt<60
"""
rs.isPay(formula)

# rs.save2paydb()
# get wide format data
rs.split_within_vars(exclude=["prac"])
#print(rs.wf.head())

aa,bb =rs.describe()
print(aa)
