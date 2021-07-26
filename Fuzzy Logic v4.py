from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import pandas as pd
import csv
import sys

def match():
    #Pulled from Database with these columns in the same order
    #Geo Level 1,	Geo Level 2,	Geo Level 3 Code,	Geo Level 3 Name,	Domain Name,	IDM COMP,	Company Name,	Industry
    filename = "C:/Users/ShatheepR/Desktop/data.csv"

    #Given by market with these columns in the same order
    #Company Name,	Industry [optional]
    filename1 = "C:/Users/ShatheepR/Desktop/comps.csv"

    score = []

    #Company Master Read
    with open(filename, encoding="utf8") as csv_file:
        reader = pd.read_csv(csv_file)
        # print(reader.head(5))
        # print(reader['IDM COMP'].count())
        # Companies needed to get mapped
        with open(filename1, encoding="utf8") as csv_source:
            reader1 = pd.read_csv(csv_source)
            for row in range(0,reader['IDM COMP'].count()):
                    Comp_Name_B = reader['Company Name'][row]
                    # print(Comp_Name_B)
#                    print("------------------------------------------")
                    for row1 in range(0,reader1['Company Name'].count()):
                        Comp_Name_A = reader1['Company Name'][row1]
                        ratio = fuzz.ratio(Comp_Name_A, Comp_Name_B)
                        partial_ratio = fuzz.partial_ratio(Comp_Name_A, Comp_Name_B)
                        token_sort_ratio = fuzz.token_sort_ratio(Comp_Name_A, Comp_Name_B)
                        token_set_ratio = fuzz.token_set_ratio(Comp_Name_A, Comp_Name_B)
                        df = pd.DataFrame({"A":[ratio, partial_ratio, token_sort_ratio, token_set_ratio]})
                        if df["A"].mean(axis = 0,skipna = True) > 50:
                            score_indiv = {
                                'URN_COMP' :reader['IDM COMP'][row],
                                'Comp_Name_Manual' : Comp_Name_A,
                                'Comp_Name_DB': Comp_Name_B,
                                'Industry': reader['Industry'][row],
                                'ratio': ratio,
                                'partial_ratio': partial_ratio,
                                'token_sort_ratio': token_sort_ratio,
                                'token_set_ratio': token_set_ratio,
                                'ratio_avg': df["A"].mean(axis = 0,skipna = True)
                            }
                            score.append(score_indiv)

        filename = f'match_file.csv'
        with open(filename, 'w') as statfile:
            writer = csv.writer(statfile, delimiter=',',lineterminator='\n')
            writer.writerow(['URN_COMP', 'Comp_Name_Manual', 'Comp_Name_DB','Industry','ratio', 'partial_ratio', 'token_sort_ratio', 'token_set_ratio', 'ratio_avg'])
            for behavior_stat in score:
                try:
    #                print(behavior_stat['URM_COMP'])
                    writer.writerow([behavior_stat['URN_COMP'], behavior_stat['Comp_Name_Manual'], behavior_stat['Comp_Name_DB'], behavior_stat['Industry'], behavior_stat['ratio'], behavior_stat['partial_ratio'], behavior_stat['token_sort_ratio'], behavior_stat['token_set_ratio'], behavior_stat['ratio_avg']])
                except:
                    continue

        print(f'Stats written to {filename}')
    #        str2Match = "Nataraj Purushothaman"
    #        strOptions = ["NP","N.P.","N P","NP."]
    #        Ratios = process.extract(str2Match,strOptions)
    #        print(Ratios)
    #        You can also select the string with the highest matching percentage
    #        highest = process.extractOne(str2Match,strOptions)
    #        print(highest)

def ranking():
    data = pd.read_csv("C:/Data/Reports/Adhoc/Audience Tribe/Lotame API/match_file.csv", encoding="utf8")
    # creating a rank column and passing the returned rank series
    data["ratio_rank"] = data.groupby(['Comp_Name_Manual'])['ratio'].rank(method='min',ascending=False)
    data["partial_ratio_rank"] = data.groupby(['Comp_Name_Manual'])["partial_ratio"].rank(method='min',ascending=False)
    data["token_sort_ratio_rank"] = data.groupby(['Comp_Name_Manual'])["token_sort_ratio"].rank(method='min',ascending=False)
    data["token_set_ratio_rank"] = data.groupby(['Comp_Name_Manual'])["token_set_ratio"].rank(method='min',ascending=False)
    data["ratio_avg_rank"] = data.groupby(['Comp_Name_Manual'])["ratio_avg"].rank(method='min',ascending=False)
    # sorting w.r.t name column
    #data.sort_values("Name", inplace=True)
    # display after sorting w.r.t Name column
    TopNRanks = 5.0
    data1 = data.loc[(data["ratio_rank"] <= TopNRanks) | (data["partial_ratio_rank"] <= TopNRanks) | (data["token_sort_ratio_rank"] <= TopNRanks) | (data["token_set_ratio_rank"] <= TopNRanks) | (data["ratio_avg_rank"] <= TopNRanks)]

    filename = f'match_filev2.csv'
    with open(filename, 'w') as statfile:
        writer = csv.writer(statfile, delimiter=',',lineterminator='\n')
        writer.writerow(['URN_COMP', 'Comp_Name_Manual', 'Comp_Name_DB','Industry','ratio', 'partial_ratio', 'token_sort_ratio', 'token_set_ratio', 'ratio_avg', 'ratio_rank', 'partial_ratio_rank', 'token_sort_ratio_rank', 'token_set_ratio_rank', 'ratio_avg_rank'])
        for behavior_stat in data1:
            try:
    #           print(behavior_stat['URM_COMP'])
                writer.writerow([behavior_stat['URN_COMP'], behavior_stat['Comp_Name_Manual'], behavior_stat['Comp_Name_DB'], behavior_stat['Industry'], behavior_stat['ratio'], behavior_stat['partial_ratio'], behavior_stat['token_sort_ratio'], behavior_stat['token_set_ratio'], behavior_stat['ratio_avg'], behavior_stat['ratio_rank'], behavior_stat['partial_ratio_rank'], behavior_stat['token_sort_ratio_rank'], behavior_stat['token_set_ratio_rank'], behavior_stat['ratio_avg_rank']])
            except:
                continue
    data1.to_csv(filename, header=True, index=False)

if __name__ == '__main__':
    match()
    ranking()