#本文的代码使用chatgpt转换，存在bug，思路还未完全理解

import re
import pandas as pd

# Define function
def inst_map(cv, inst_dict, db="wos"):
    if db == "wos":
        cv = pd.merge(cv, inst_dict[['inst', 'wos']].drop_duplicates(), on="inst", how="inner")
        cv = cv[['wos', 'startyear', 'endyear']].drop_duplicates()
    elif db == "scopus":
        cv = pd.merge(cv, inst_dict[['inst', 'scopus']].drop_duplicates(), on="inst", how="inner")
        cv = cv[['scopus', 'startyear', 'endyear']].drop_duplicates()
    elif db == "openalex":
        cv = pd.merge(cv, inst_dict[['inst', 'openalex']].drop_duplicates(), on="inst", how="inner")
        cv = cv[['openalex', 'startyear', 'endyear']].drop_duplicates()
    else:
        print("The database is not supported by now, please contact the authors on Github.")
        return None

    cv.columns = ["inst", "startyear", "endyear"]
    return cv

# Define function
def cv_filter(paper, cv, year_lag=2):
    paper['aff'] = paper['aff'].str.lower()
    paper['pub_year'] = pd.to_numeric(paper['pub_year'], errors='coerce')

    cv['inst'] = cv['inst'].str.lower()
    cv['startyear'] = pd.to_numeric(cv['startyear'], errors='coerce')
    cv['endyear'] = pd.to_numeric(cv['endyear'], errors='coerce')

    result = pd.merge(paper.assign(key=1), cv.assign(key=1), on='key').drop('key', axis=1)
    result = result[(result['pub_year'] >= result['startyear']) &
                    (result['pub_year'] <= result['endyear'] + year_lag) &
                    (result['aff'].str.contains(result['inst'], flags=re.IGNORECASE, regex=True))]

    result = result['pid'].unique()

    return result

#第三个函数通过引⽤添加⼀次操作，选出有引⽤关系的pid，循环放⼊主函数
def cite_glue(pid, cite):
    pid_1 = cite[cite['citing_pid'].isin(pid)]['cited_pid'].unique()
    pid_2 = cite[cite['cited_pid'].isin(pid)]['citing_pid'].unique()

    pid_add = pd.Series(pd.concat([pd.Series(pid_1), pd.Series(pid_2)], ignore_index=True).unique())
    pid_add = pid_add[~pid_add.isin(pid)].tolist()

    return pid_add

#第四个是主函数，完成所有的操作

def cv_disam(paper, cv, year_lag1=2, year_lag2=2, cite):
    # 第⼀步筛选
    paper_1 = paper[paper['initials'] == 0]
    paper_2 = paper[paper['initials'] == 1]

    pid_stage1 = cv_filter(paper_1, cv, year_lag1)
    cite = cite[(cite['citing_pid'].isin(pid_stage1)) | (cite['cited_pid'].isin(pid_stage1)) | (
        cite['citing_pid'].isin(paper_2['pid'])) | (cite['cited_pid'].isin(paper_2['pid']))]

    pid_core = pid_stage1

    # 第⼆步循环添加
    while True:
        pid_add = cite_glue(pid_core, cite)
        if len(pid_add) == 0:
            break
        else:
            pid_core.extend(pid_add)

    # 第三步对pid_stage2使⽤cv筛选
    pid_stage2 = list(set(pid_core) - set(pid_stage1))

    paper_3 = paper_2[paper_2['pid'].isin(pid_stage2)]
    pid_stage3 = cv_filter(paper_3, cv, year_lag2)

    pid_disam = list(set(pid_stage1).union(pid_stage3))

    return pid_disam


# Read CSV files
scientist = pd.read_csv("scientist.csv")
inst_wos_dict = pd.read_csv("inst_wos_dict.csv")
cddt_paper = pd.read_csv("cddt_paper.csv")
cite_all = pd.read_csv("cite.csv")

# Perform data manipulations
scientist['inst'] = scientist['inst'].str.lower()
inst_wos_dict['inst'] = inst_wos_dict['inst'].str.lower()
cite_all.columns = ["citing_pid", "cited_pid"]

# Display the modified DataFrames
print(scientist)
print(inst_wos_dict)
print(cddt_paper)
print(cite_all)

# Initialize an empty DataFrame for the result
result = pd.DataFrame(columns=["uniqueID", "pid"])

# Iterate over unique values of 'uniqueID'
for unique_id in scientist['uniqueID'].unique():
    # Filter data for the current 'uniqueID'
    cv = scientist[scientist['uniqueID'] == unique_id].copy()
    cv = inst_map(cv=cv, inst_dict=inst_wos_dict)

    paper = cddt_paper[(cddt_paper['uniqueID'] == unique_id) & (cddt_paper['item_type'] == "Article")]
    paper['initials'] = pd.to_numeric(paper['type'] == 2)
    paper = paper.rename(columns={'ut_char': 'pid', 'addr': 'aff'})
    paper = paper[['pid', 'aff', 'pub_year', 'initials']].drop_duplicates()

    cite = cite_all[cite_all['citing_pid'].isin(paper['pid']) & cite_all['cited_pid'].isin(paper['pid'])]

    pid_disam = cv_disam(paper, cv, year_lag1=2, year_lag2=2, cite)

    # Append the result to the 'result' DataFrame
    result = pd.concat([result, pd.DataFrame({'uniqueID': [unique_id], 'pid': [pid_disam[0]]})])

    print(unique_id)

# Reset the index of the 'result' DataFrame
result.reset_index(drop=True, inplace=True)

# Display the 'result' DataFrame
print(result)
