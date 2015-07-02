
# coding: utf-8
# Monica Pangilinan
# Again rough -> this takes the variables that were made 
# from the csv files and averages it for up to 5 previous games


#take out the info I have from my sql database
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.types import String

#This takes code from fellow fellow to access the database.  Thanks Alex!
"""
Created on Mon Jun  8 12:46:29 2015                                                                            
                                                                                                               
@author: alexsutherland                                                                                        
"""

#Note: This is for accessing data with the pymysql driver                                                      

import sqlalchemy
import pandas as pd

#Your database info: for qb
username='root'
password=''
server='localhost'
databaseName='qb'
tableName='qb_2002_2012'

#Create connection with sql database                                                                           
engineString = 'mysql+pymysql://'+ username +':'+ password +'@'+ server +'/'+databaseName
sqlEngine = sqlalchemy.create_engine(engineString)
con = sqlEngine.connect()

#Generate/execute query                                                                                        
query = "SELECT * FROM " + tableName  + " WHERE Tie = 0 ORDER BY QB, DatePlayed"
sqlResult = con.execute(query)

#Create dataframe from result for train set
df_qb = pd.DataFrame(sqlResult.fetchall())
df_qb.columns = sqlResult.keys()


con.close()

#for defense
databaseName='defense'
tableName='defense_2002_2012'

#Create connection with sql database                                                                           
engineString = 'mysql+pymysql://'+ username +':'+ password +'@'+ server +'/'+databaseName
sqlEngine = sqlalchemy.create_engine(engineString)
con = sqlEngine.connect()

#Generate/execute query                                                                                        
query = "SELECT * FROM " + tableName  + " WHERE Tie = 0 ORDER BY Defense, DatePlayed"
sqlResult = con.execute(query)

#Create dataframe from result for train set
df_defense = pd.DataFrame(sqlResult.fetchall())
df_defense.columns = sqlResult.keys()


con.close()

#for receiver
databaseName='receiver'
tableName='receiver_2002_2012'

#Create connection with sql database                                                                           
engineString = 'mysql+pymysql://'+ username +':'+ password +'@'+ server +'/'+databaseName
sqlEngine = sqlalchemy.create_engine(engineString)
con = sqlEngine.connect()

#Generate/execute query                                                                                        
query = "SELECT * FROM " + tableName  + " WHERE Tie = 0 ORDER BY Receiver, DatePlayed"
sqlResult = con.execute(query)

#Create dataframe from result for train set
df_receiver = pd.DataFrame(sqlResult.fetchall())
df_receiver.columns = sqlResult.keys()


con.close()

#for RB
databaseName='RB'
tableName='RB_2002_2012'

#Create connection with sql database                                                                           
engineString = 'mysql+pymysql://'+ username +':'+ password +'@'+ server +'/'+databaseName
sqlEngine = sqlalchemy.create_engine(engineString)
con = sqlEngine.connect()

#Generate/execute query                                                                                        
query = "SELECT * FROM " + tableName  + " WHERE Tie = 0 ORDER BY Team, DatePlayed"
sqlResult = con.execute(query)

#Create dataframe from result for train set
df_RB = pd.DataFrame(sqlResult.fetchall())
df_RB.columns = sqlResult.keys()


con.close()

#Time for the qb:
#do the averages from last 5 games including the year before
all_qb_array = df_qb['QB'].unique()

#what I will put into the sql database -> dumb haha
target_date_all = []
target_game_id = []
ave_team_all = []
ave_qb_all = []
ave_qbr_all = []
ave_att_all = []
ave_comp_all = []
ave_yds_all = []
ave_td_all = []
ave_inter_all = []
ave_sacks_all = []
ave_sack_yds_all = []
ave_rush_all = []
ave_rush_yds_all = []
ave_fumble_all = []
ave_score_all = []
ave_opp_score_all = []
ave_win_all = []
target_win_all = []
opp_team_all = []
count_check = 0
for qb in all_qb_array:

    qb_indiv_df = df_qb[df_qb.QB == qb]

    if qb_indiv_df.empty:
        print('DataFrame is empty!')
    else: #continue since that player exists

        for date in qb_indiv_df.DatePlayed:
            index_game_played = qb_indiv_df[qb_indiv_df['DatePlayed'] <= date].index.tolist()

            if len(index_game_played) >= 2: #calculate all the mean values since have at least 2 games
                last_5_game_info = qb_indiv_df.loc[index_game_played[:-1]]
                
                #if have at least 5 previous games, keep only the last 5
                if len(index_game_played) >= 6:
                    last_5_game_info = qb_indiv_df.loc[index_game_played[-6:-1]]
                
                target_game_id.append(qb_indiv_df.GameID[index_game_played[-1]])
                ave_team_all.append(qb_indiv_df.Team[index_game_played[-1]])
                target_date_all.append(qb_indiv_df.DatePlayed[index_game_played[-1]])
                target_win_all.append(qb_indiv_df.Win[index_game_played[-1]])
                ave_qb_all.append(qb)
                if qb_indiv_df.GameID[index_game_played[-1]] == '20120930_WAS@TB':
                    if qb_indiv_df.Team[index_game_played[-1]] == 'WAS' :
                        opp_team_all.append('TB')
                    else:
                        opp_team_all.append('WAS')
                else:
                    opp_team_all.append(qb_indiv_df.OppTeam[index_game_played[-1]])

                #make the mean values
                ave_values_df = last_5_game_info.mean(axis=0)
                ave_qbr_all.append(ave_values_df.QBRating)
                ave_att_all.append(ave_values_df.Attempts)
                ave_comp_all.append(ave_values_df.Completions)
                ave_yds_all.append(ave_values_df.TotYds)
                ave_td_all.append(ave_values_df.TotTD)
                ave_inter_all.append(ave_values_df.TotInterception)
                ave_score_all.append(ave_values_df.Score)
                ave_opp_score_all.append(ave_values_df.OppScore)
                ave_win_all.append(ave_values_df.Win)
                ave_sacks_all.append(ave_values_df.TotSacks)
                ave_sack_yds_all.append(ave_values_df.SackYds)
                ave_rush_all.append(ave_values_df.TotRush)
                ave_rush_yds_all.append(ave_values_df.RushYds)
                ave_fumble_all.append(ave_values_df.Fumbles)
                
            else:
               count_check = count_check + 1
                                               
print "Missing " + str(count_check) + " qb games"      

#put into a data frame
data_qb_recast = {'DatePlayed': target_date_all,
        'GameID': target_game_id,
        'QB': ave_qb_all,
        'Team': ave_team_all,
        'OppTeam': opp_team_all,
        'Win': target_win_all,
        'AveQBRating': ave_qbr_all ,
        'AveAttempts': ave_att_all,
        'AveCompletions': ave_comp_all,
        'AveTotYds': ave_yds_all,
        'AveTotTD': ave_td_all,
        'AveTotInterception': ave_inter_all,
        'AveScore': ave_score_all,
        'AveOppScore': ave_opp_score_all,
        'AveWin': ave_win_all,
        'AveTotSacks': ave_sacks_all,
        'AveSackYds': ave_sack_yds_all,
        'AveTotRush': ave_rush_all,
        'AveRushYds': ave_rush_yds_all,
        'AveFumbles': ave_fumble_all}

count_multiples = 0
qb_recast_frame = pd.DataFrame(data_qb_recast,columns=['DatePlayed','GameID','QB','Team','OppTeam','Win',                                                       'AveQBRating','AveAttempts',                                                       'AveCompletions','AveTotYds','AveTotTD',                                                       'AveTotInterception','AveScore','AveOppScore',                                                       'AveWin','AveTotSacks','AveSackYds','AveTotRush',                                                       'AveRushYds', 'AveFumbles'],dtype =float)

#Now looking at the opponent's QB
opp_ave_qbr_all = []
opp_ave_att_all = []
opp_ave_comp_all = []
opp_ave_yds_all = []
opp_ave_td_all = []
opp_ave_inter_all = []
opp_ave_sacks_all = []
opp_ave_sack_yds_all = []
opp_ave_rush_all = []
opp_ave_rush_yds_all = []
opp_ave_fumble_all = []
opp_ave_score_all = []
opp_ave_opp_score_all = []
opp_ave_win_all = []

for count,game in enumerate(qb_recast_frame.GameID):
    opp_team = qb_recast_frame[(qb_recast_frame['GameID'] == game) & (qb_recast_frame['Team'] == qb_recast_frame.OppTeam[count])]
    if len(opp_team) == 0:
        opp_team = qb_recast_frame[(qb_recast_frame['DatePlayed'] < qb_recast_frame.DatePlayed[count]) &  (qb_recast_frame['Team'] == qb_recast_frame.OppTeam[count])]
        
        if len(opp_team) > 0:
            opp_team = opp_team[opp_team.DatePlayed == max(np.array(opp_team.DatePlayed))]
                  
    if len(opp_team) > 1:
        opp_team = opp_team[opp_team.AveAttempts == max(np.array(opp_team.AveAttempts))]
        
        if len(opp_team) > 1:
            opp_team = opp_team[opp_team.AveQBRating == max(np.array(opp_team.AveQBRating))]
    if len(opp_team) == 1:
        opp_ave_qbr_all.append(opp_team.AveQBRating)
        opp_ave_att_all.append(opp_team.AveAttempts)
        opp_ave_comp_all.append(opp_team.AveCompletions)
        opp_ave_yds_all.append(opp_team.AveTotYds)
        opp_ave_td_all.append(opp_team.AveTotTD)
        opp_ave_inter_all.append(opp_team.AveTotInterception)
        opp_ave_score_all.append(opp_team.AveScore)
        opp_ave_opp_score_all.append(opp_team.AveOppScore)
        opp_ave_win_all.append(opp_team.AveWin)
        opp_ave_sacks_all.append(opp_team.AveTotSacks)
        opp_ave_sack_yds_all.append(opp_team.AveSackYds)
        opp_ave_rush_all.append(opp_team.AveTotRush)
        opp_ave_rush_yds_all.append(opp_team.AveRushYds)
        opp_ave_fumble_all.append(opp_team.AveFumbles)
    elif len(opp_team) > 1:
        
        counter = counter + 1
        opp_ave_qbr_all.append(-1)
        opp_ave_att_all.append(-1)
        opp_ave_comp_all.append(-1)
        opp_ave_yds_all.append(-1)
        opp_ave_td_all.append(-1)
        opp_ave_inter_all.append(-1)
        opp_ave_score_all.append(-1)
        opp_ave_opp_score_all.append(-1)
        opp_ave_win_all.append(-1)
        opp_ave_sacks_all.append(-1)
        opp_ave_sack_yds_all.append(-1)
        opp_ave_rush_all.append(-1)
        opp_ave_rush_yds_all.append(-1)
        opp_ave_fumble_all.append(-1)        
    elif len(opp_team) == 0:
        opp_ave_qbr_all.append(-1)
        opp_ave_att_all.append(-1)
        opp_ave_comp_all.append(-1)
        opp_ave_yds_all.append(-1)
        opp_ave_td_all.append(-1)
        opp_ave_inter_all.append(-1)
        opp_ave_score_all.append(-1)
        opp_ave_opp_score_all.append(-1)
        opp_ave_win_all.append(-1)
        opp_ave_sacks_all.append(-1)
        opp_ave_sack_yds_all.append(-1)
        opp_ave_rush_all.append(-1)
        opp_ave_rush_yds_all.append(-1)
        opp_ave_fumble_all.append(-1)
           

#make the panda dataframe
data_qb_recast = {'DatePlayed': target_date_all,
        'GameID': target_game_id,
        'QB': ave_qb_all,
        'Team': ave_team_all,
        'OppTeam': opp_team_all,
        'Win': target_win_all,
        'AveQBRating': ave_qbr_all ,
        'AveAttempts': ave_att_all,
        'AveCompletions': ave_comp_all,
        'AveTotYds': ave_yds_all,
        'AveTotTD': ave_td_all,
        'AveTotInterception': ave_inter_all,
        'AveScore': ave_score_all,
        'AveOppScore': ave_opp_score_all,
        'AveWin': ave_win_all,
        'AveTotSacks': ave_sacks_all,
        'AveSackYds': ave_sack_yds_all,
        'AveTotRush': ave_rush_all,
        'AveRushYds': ave_rush_yds_all,
        'AveFumbles': ave_fumble_all,
        'OppAveQBRating': opp_ave_qbr_all ,
        'OppAveAttempts': opp_ave_att_all,
        'OppAveCompletions': opp_ave_comp_all,
        'OppAveTotYds': opp_ave_yds_all,
        'OppAveTotTD': opp_ave_td_all,
        'OppAveTotInterception': opp_ave_inter_all,
        'OppAveScore': opp_ave_score_all,
        'OppAveOppScore': opp_ave_opp_score_all,
        'OppAveWin': opp_ave_win_all,
        'OppAveTotSacks': opp_ave_sacks_all,
        'OppAveSackYds': opp_ave_sack_yds_all,
        'OppAveTotRush': opp_ave_rush_all,
        'OppAveRushYds': opp_ave_rush_yds_all,
        'OppAveFumbles': opp_ave_fumble_all}

qb_recast_frame = pd.DataFrame(data_qb_recast,columns=['DatePlayed','GameID','QB','Team', 'OppTeam','Win',                                                       'AveQBRating','AveAttempts',                                                       'AveCompletions','AveTotYds','AveTotTD',                                                       'AveTotInterception','AveScore','AveOppScore',                                                       'AveWin','AveTotSacks','AveSackYds','AveTotRush',                                                       'AveRushYds', 'OppAveRushYds','OppAveTotSacks',                                                       'OppAveSackYds','OppAveTotRush','OppAveFumbles',                                                       'AveFumbles','OppAveQBRating','OppAveAttempts',                                                       'OppAveCompletions','OppAveTotYds','OppAveTotTD',                                                       'OppAveTotInterception','OppAveScore','OppAveOppScore',                                                       'OppAveWin'],dtype =float)



from sqlalchemy.dialects.mysql import DOUBLE

engine = create_engine("mysql+pymysql://root:@localhost/qb")
#qb_recast_frame.to_sql('qb_recast_2002_2012_ave_all_season', engine, index=False, if_exists='replace',dtype={'DatePlayed':DOUBLE})

print "FINISHED WITH QB"


#now for RB
#do the averages from last 5 games including the year before
all_RB_array = df_RB['RB'].unique()

#what I will put into the sql database -> dumb haha
target_date_all = []
target_game_id = []
ave_team_all = []
ave_RB_all = []
ave_att_all = []
ave_yds_all = []
ave_pass_yds_all = []
ave_pass_att_all = []
ave_td_all = []
ave_inter_all = []
ave_fumble_all = []
target_win_all = []
opp_team_all = []
count_check = 0

for RB in all_RB_array:
    RB_indiv_df = df_RB[df_RB.RB == RB]


    if RB_indiv_df.empty:
        print('DataFrame is empty!')
    else: #continue since that player exists

        for date in RB_indiv_df.DatePlayed:

            index_game_played = RB_indiv_df[RB_indiv_df['DatePlayed'] <= date].index.tolist()

            if len(index_game_played) >= 2: #calculate all the mean values since have at least 1 game
                last_5_game_info = RB_indiv_df.loc[index_game_played[:-1]]
                if len(index_game_played) >= 6:
                    last_5_game_info = RB_indiv_df.loc[index_game_played[-6:-1]]
                
            
                target_game_id.append(RB_indiv_df.GameID[index_game_played[-1]])
                ave_team_all.append(RB_indiv_df.Team[index_game_played[-1]])
                target_date_all.append(RB_indiv_df.DatePlayed[index_game_played[-1]])
                target_win_all.append(RB_indiv_df.Win[index_game_played[-1]])
                ave_RB_all.append(RB)

                if RB_indiv_df.GameID[index_game_played[-1]] == '20120930_WAS@TB':
                    if RB_indiv_df.Team[index_game_played[-1]] == 'WAS' :
                        opp_team_all.append('TB')
                    else:
                        opp_team_all.append('WAS')
                else:
                    opp_team_all.append(RB_indiv_df.OppTeam[index_game_played[-1]])

                #make the mean values
                ave_values_df = last_5_game_info.mean(axis=0)
                ave_att_all.append(ave_values_df.Attempts)
                ave_yds_all.append(ave_values_df.TotYds)
                ave_pass_yds_all.append(ave_values_df.TotPassYds)
                ave_td_all.append(ave_values_df.TotTD)
                ave_pass_att_all.append(ave_values_df.PassAttempts)
                ave_inter_all.append(ave_values_df.TotInterception)
                ave_fumble_all.append(ave_values_df.Fumbles)
                
            else:
                count_check = count_check + 1
                
                
                
print "Missing " + str(count_check) + " RB games"      
        
#make the panda dataframe
data_RB_recast = {'DatePlayed': target_date_all,
        'GameID': target_game_id,
        'RB': ave_RB_all,
        'Team': ave_team_all,
        'OppTeam': opp_team_all,
        'Win': target_win_all,
        'AveAttempts': ave_att_all,
        'AveTotYds': ave_yds_all,
        'AvePassAttempts': ave_pass_att_all,
        'AvePassTotYds': ave_pass_yds_all,
        'AveTotTD': ave_td_all,
        'AveTotInterception': ave_inter_all,
        'AveFumbles': ave_fumble_all}

RB_recast_frame = pd.DataFrame(data_RB_recast,columns=['DatePlayed','GameID','RB','Team','OppTeam',                                                       'Win','AveAttempts','AvePassAttempts',                                                       'AveTotYds','AvePassTotYds','AveTotTD',                                                       'AveTotInterception',                                                       'AveFumbles'],dtype =float)


#now cross match for the opposing team haha
opp_ave_att_all = []
opp_ave_yds_all = []
opp_ave_pass_yds_all = []
opp_ave_pass_att_all = []
opp_ave_td_all = []
opp_ave_inter_all = []
opp_ave_fumble_all = []

count_multiples = 0

counter = 0
counter_1 = 0
for count,game in enumerate(RB_recast_frame.GameID):
    opp_team = RB_recast_frame[(RB_recast_frame['GameID'] == game) &                                (RB_recast_frame['Team'] == RB_recast_frame.OppTeam[count])]
    if len(opp_team) == 0:
        opp_team = RB_recast_frame[(RB_recast_frame['DatePlayed'] < RB_recast_frame.DatePlayed[count]) &                                (RB_recast_frame['Team'] == RB_recast_frame.OppTeam[count])]
        
        if len(opp_team) > 0:
            opp_team = opp_team[opp_team.DatePlayed == max(np.array(opp_team.DatePlayed))]
                  
    if len(opp_team) > 1:
        opp_team = opp_team[opp_team.AveAttempts == max(np.array(opp_team.AveAttempts))]
        
        if len(opp_team) > 1:
            opp_team = opp_team[opp_team.AveTotYds == max(np.array(opp_team.AveTotYds))]
        
    if len(opp_team) == 1:
        opp_ave_att_all.append(opp_team.AveAttempts)
        opp_ave_yds_all.append(opp_team.AveTotYds)
        opp_ave_pass_att_all.append(opp_team.AvePassAttempts)
        opp_ave_pass_yds_all.append(opp_team.AvePassTotYds)
        opp_ave_td_all.append(opp_team.AveTotTD)
        opp_ave_inter_all.append(opp_team.AveTotInterception)
        opp_ave_fumble_all.append(opp_team.AveFumbles)
        
        
    elif len(opp_team) > 1:
        counter = counter + 1
        opp_ave_att_all.append(-1)
        opp_ave_yds_all.append(-1)
        opp_ave_pass_yds_all.append(-1)
        opp_ave_pass_att_all.append(-1)
        opp_ave_td_all.append(-1)
        opp_ave_inter_all.append(-1)
        opp_ave_score_all.append(-1)
        opp_ave_opp_score_all.append(-1)
        opp_ave_win_all.append(-1)
        opp_ave_fumble_all.append(-1)
    elif len(opp_team) == 0:
        counter_1 = counter_1 + 1
        opp_ave_att_all.append(-1)
        opp_ave_yds_all.append(-1)
        opp_ave_pass_yds_all.append(-1)
        opp_ave_pass_att_all.append(-1)
        opp_ave_td_all.append(-1)
        opp_ave_inter_all.append(-1)
        opp_ave_score_all.append(-1)
        opp_ave_opp_score_all.append(-1)
        opp_ave_win_all.append(-1)
        opp_ave_fumble_all.append(-1)

#make the panda dataframe
data_RB_recast = {'DatePlayed': target_date_all,
        'GameID': target_game_id,
        'RB': ave_RB_all,
        'Team': ave_team_all,
        'OppTeam': opp_team_all,
        'Win': target_win_all,
        'AveAttempts': ave_att_all,
        'AveTotYds': ave_yds_all,
        'AvePassAttempts': ave_pass_att_all,
        'AvePassTotYds': ave_pass_yds_all,
        'AveTotTD': ave_td_all,
        'AveTotInterception': ave_inter_all,
        'AveFumbles': ave_fumble_all,
        'OppAveAttempts': opp_ave_att_all,
        'OppAveTotYds': opp_ave_yds_all,
        'OppAvePassAttempts': opp_ave_pass_att_all,
        'OppAvePassTotYds': opp_ave_pass_yds_all,
        'OppAveTotTD': opp_ave_td_all,
        'OppAveTotInterception': opp_ave_inter_all,
        'OppAveFumbles': opp_ave_fumble_all}

RB_recast_frame = pd.DataFrame(data_RB_recast,columns=['DatePlayed','GameID','RB','Team','OppTeam','Win',                                                       'AveAttempts','AvePassAttempts',                                                       'AveTotYds','AvePassTotYds','AveTotTD',                                                       'AveTotInterception','OppAveFumbles',                                                       'AveFumbles','OppAveAttempts',                                                       'OppAveTotYds','OppAveTotTD',                                                       'OppAvePassAttempts',                                                       'OppAvePassTotYds','OppAveTotInterception'],dtype =float)

engine = create_engine("mysql+pymysql://root:@localhost/RB")
#RB_recast_frame.to_sql('RB_recast_2002_2012_ave_all_season', engine, index=False, if_exists='replace',dtype={'DatePlayed':DOUBLE})

#now defense portion
#do the averages from last 5 games including the year before
all_defense_array = df_defense['Defense'].unique()

#what I will put into the sql database -> dumb haha
target_date_all = []
target_game_id = []
ave_pass_yds_all = []
ave_rush_yds_all = []
ave_inter_all = []
ave_fumble_all = []
ave_sacks_all = []
ave_sack_yds_all = []
target_win_all = []
ave_defense_all = []
opp_team_all = []

count_check = 0


for defense in all_defense_array:
    defense_indiv_df = df_defense[df_defense.Defense == defense]

    if defense_indiv_df.empty:
        print('DataFrame is empty!')
    else: #continue since that player exists

        for date in defense_indiv_df.DatePlayed:
            index_game_played = defense_indiv_df[defense_indiv_df['DatePlayed'] <= date].index.tolist()
            
            if len(index_game_played) >= 2: 
                last_5_game_info = defense_indiv_df.loc[index_game_played[:-1]]
                if len(index_game_played) >= 6:
                    last_5_game_info =  defense_indiv_df.loc[index_game_played[-6:-1]]
            
                target_game_id.append(defense_indiv_df.GameID[index_game_played[-1]])
                target_date_all.append(defense_indiv_df.DatePlayed[index_game_played[-1]])
                target_win_all.append(defense_indiv_df.Win[index_game_played[-1]])
                ave_defense_all.append(defense)
                opp_team_all.append(defense_indiv_df.OppTeam[index_game_played[-1]])

                #make the mean values
                ave_values_df = last_5_game_info.mean(axis=0)
                ave_pass_yds_all.append(ave_values_df.TotPassYds)
                ave_rush_yds_all.append(ave_values_df.TotRushYds)
                ave_inter_all.append(ave_values_df.TotInterception)
                ave_fumble_all.append(ave_values_df.Fumbles)
                ave_sacks_all.append(ave_values_df.TotSacks)
                ave_sack_yds_all.append(ave_values_df.SackYds)
                
            else:
                 count_check = count_check + 1
                
                
                
print "Missing " + str(count_check) + " Defense games"      
        
#make the panda dataframe
data_defense_recast = {'DatePlayed': target_date_all,
        'GameID': target_game_id,
        'Defense': ave_defense_all,
        'OppTeam': opp_team_all,
        'Win': target_win_all,
        'AveTotPassYds': ave_pass_yds_all,
        'AveTotRushYds': ave_rush_yds_all,
        'AveTotInterception': ave_inter_all,
        'AveTotSacks': ave_sacks_all,
        'AveSackYds': ave_sack_yds_all,
        'AveFumbles': ave_fumble_all}

defense_recast_frame = pd.DataFrame(data_defense_recast,columns=['DatePlayed','GameID','Defense','OppTeam','Win',                                                       'AveTotPassYds','AveTotRushYds','AveTotInterception',                                                       'AveTotSacks','AveSackYds',                                                                 'AveFumbles'],dtype =float)

                       
#now cross match for the opposing team haha
opp_ave_pass_yds_all = []
opp_ave_rush_yds_all = []
opp_ave_inter_all = []
opp_ave_fumble_all = []
opp_ave_sacks_all = []
opp_ave_sack_yds_all = []
count_multiples = 0

counter = 0
counter_1 = 0

for count,game in enumerate(defense_recast_frame.GameID):
    opp_team = defense_recast_frame[(defense_recast_frame['GameID'] == game) &                                (defense_recast_frame['Defense'] == defense_recast_frame.OppTeam[count])]
    if len(opp_team) == 0:
        opp_team = defense_recast_frame[(defense_recast_frame['DatePlayed'] < defense_recast_frame.DatePlayed[count]) &                                (defense_recast_frame['Defense'] == defense_recast_frame.OppTeam[count])]
        
        if len(opp_team) > 0:
            opp_team = opp_team[opp_team.DatePlayed == max(np.array(opp_team.DatePlayed))]
                  
    if len(opp_team) > 1:
        opp_team = opp_team[opp_team.AveTotPassYds == max(np.array(opp_team.AveTotPassYds))]
        
        if len(opp_team) > 1:
            opp_team = opp_team[opp_team.AveTotRushYds == max(np.array(opp_team.AveTotRushYds))]
        
    if len(opp_team) == 1:
        opp_ave_pass_yds_all.append(opp_team.AveTotPassYds)
        opp_ave_rush_yds_all.append(opp_team.AveTotRushYds)
        opp_ave_inter_all.append(opp_team.AveTotInterception)
        opp_ave_fumble_all.append(opp_team.AveFumbles)
        opp_ave_sacks_all.append(opp_team.AveTotSacks)
        opp_ave_sack_yds_all.append(opp_team.AveSackYds)
        
    elif len(opp_team) > 1:
        counter = counter + 1
        opp_ave_pass_yds_all.append(-1)
        opp_ave_rush_yds_all.append(-1)
        opp_ave_inter_all.append(-1)
        opp_ave_fumble_all.append(-1)
        opp_ave_sacks_all.append(-1)
        opp_ave_sack_yds_all.append(-1)
 
       
    elif len(opp_team) == 0:
        print game
        counter_1 = counter_1 + 1
        opp_ave_pass_yds_all.append(-1)
        opp_ave_rush_yds_all.append(-1)
        opp_ave_inter_all.append(-1)
        opp_ave_fumble_all.append(-1)
        opp_ave_sacks_all.append(-1)
        opp_ave_sack_yds_all.append(-1)
           

print counter, counter_1 



#make the panda dataframe
data_defense_recast = {'DatePlayed': target_date_all,
        'GameID': target_game_id,
        'Defense': ave_defense_all,
        'Win': target_win_all,
        'AveTotPassYds': ave_pass_yds_all,
        'AveTotRushYds': ave_rush_yds_all,
        'AveTotInterception': ave_inter_all,
        'AveTotSacks': ave_sacks_all,
        'AveSackYds': ave_sack_yds_all,
        'AveFumbles': ave_fumble_all,
        'OppAveTotPassYds': opp_ave_pass_yds_all,
        'OppAveTotRushYds': opp_ave_rush_yds_all,
        'OppAveTotInterception': opp_ave_inter_all,
        'OppAveTotSacks': opp_ave_sacks_all,
        'OppAveSackYds': opp_ave_sack_yds_all,
        'OppAveFumbles': opp_ave_fumble_all}

defense_recast_frame = pd.DataFrame(data_defense_recast,columns=['DatePlayed','GameID','Defense','Win',                                                       'AveTotPassYds','AveTotRushYds','AveTotInterception',                                                       'AveTotSacks','AveSackYds','AveFumbles',                                                        'OppAveTotPassYds','OppAveTotRushYds',                                                        'OppAveTotInterception','OppAveTotSacks',                                                        'OppAveSackYds','OppAveFumbles'],dtype =float)


engine = create_engine("mysql+pymysql://root:@localhost/defense")
#defense_recast_frame.to_sql('defense_recast_2002_2012_ave_all_season', engine, index=False, if_exists='replace',dtype={'DatePlayed':DOUBLE})

