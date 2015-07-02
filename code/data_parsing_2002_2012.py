# coding: utf-8
# Monica Pangilinan
# v0.0 Data parsing from the csv files.  Pretty rough but works.  
# Will fix to make it much modular

import pandas as pd
import numpy as np 
import re
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from sqlalchemy.types import String
from sqlalchemy.dialects.mysql import DOUBLE

#Function to put lower/upper bounds for quarterback rating
def qb_bound_fix(ab):
    if ab < 0:
        ab = 0
    elif ab > 2.375:
        ab = 2.375        
    return ab

#Calculate qb-rating
def calc_qb_rating(ATT,COMP,YDS,TD,INT):    
    ab = (COMP/float(ATT) - 0.3)*5 
    bb = (YDS/float(ATT) - 3)*.25
    cb = (TD/float(ATT))*20
    db = 2.375 - (INT/float(ATT)*25)
    
    abfix = qb_bound_fix(ab)
    bbfix = qb_bound_fix(bb)
    cbfix = qb_bound_fix(cb)
    dbfix = qb_bound_fix(db)
    qb_rating = 100*(abfix+bbfix+cbfix+dbfix)/6
    return qb_rating


#Go through the years and extract the data from messy csv files
years = [2002] #,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012]
for year in years:
    file='../data_football/' + str(year) + '_nfl_pbp_data.csv'
    
    data_football = pd.read_csv(file, header = False )

    #find unique games and put the info in there for the different positions
    unique_games = np.unique(data_football.gameid[:])
    unique_game_scores = {}


    for game in unique_games:
        index_data_football = data_football[data_football['gameid'] == game].index.tolist()[-1]


        #Structure for unique_game_scores = [winning team, losing team, winning score, losing score, tie = 1 or no tie = 0]
        if data_football.offscore[index_data_football] >  data_football.defscore[index_data_football]:
            unique_game_scores[game] = [data_football.off[index_data_football],data_football.defn[index_data_football],                                        data_football.offscore[index_data_football],data_football.defscore[index_data_football],0]

        elif data_football.offscore[index_data_football] ==  data_football.defscore[index_data_football]:
            unique_game_scores[game] = [data_football.off[index_data_football],data_football.defn[index_data_football],                                        data_football.offscore[index_data_football],data_football.defscore[index_data_football],1]
        else:
            unique_game_scores[game] = [data_football.defn[index_data_football],data_football.off[index_data_football],                                        data_football.defscore[index_data_football],data_football.offscore[index_data_football],0]

    print "Running through all the lines"
    unique_gameinfo_qb_fumble = {}
    unique_gameinfo_receiver_fumble = {}
    unique_gameinfo_RB_fumble = {}
    unique_gameinfo_qb = {} #[(ATT , COMP , YDS , TD , INT ,# sacks, # sack yds)]
    unique_gameinfo_receiver = {} #[(ATT , COMP , YDS , TD , INT ,# sacks, # sack yds)]
    unique_gameinfo_def = {} #[(# of fumbles, tot_pass_yds , tot_rushing_yds, # INT ,# sacks, # sack yds, # pts allowed)]
    unique_gameinfo_RB = {}
    no_value = np.array([0,0,0,0,0,0,0])
    count_special_cases = 0

    for count, unclean_description in enumerate(data_football.description): #go through each play-by-play in the file

        description = ' '.join(unclean_description.split())

        gameid = data_football.gameid[count]
        team = data_football.off[count]
        opp_team = data_football.defn[count]
        quarterback = ""
        receiver = ""
        RB = ""
        if (opp_team,gameid) not in unique_gameinfo_def:
            unique_gameinfo_def[(opp_team,gameid)] = no_value

        #Keep text after reverse is found.  The play is the text after "REVERSED"
        if re.search(r'REVERSE', description) :
            description = description[description.find('REVERSED.')+9:]
            
        #Search through the fumbles per position    
        if re.search(r'FUMBLES', description) and not re.search(r'REVERSE', description) :

            unique_gameinfo_def[(opp_team,gameid)] = unique_gameinfo_def[(opp_team,gameid)] + np.array([1,0,0,0,0,0,0])
            fumble_description = description[: description.find('FUMBLES')]

            if re.search(r' pass ',fumble_description): #fumble related to qb
                quarterback_string = fumble_description[:fumble_description.find(' pass ')]

                if re.search(r'(Shotgun)',quarterback_string):
                    quarterback_string = quarterback_string[quarterback_string.find('(Shotgun) ')+10:]

                quarterback_string = quarterback_string.replace(" ", "")
                if re.search(r'[A-Z][a-z]*\.[A-Z\w]*',quarterback_string):
                    quarterback = re.search(r'[A-Z][a-z]*\.[A-Z\w]*',quarterback_string).group()
                elif re.search(r'Alex Smith', fumble_description):           
                    quarterback = 'A.Smith'
                elif re.search(r' Ryan ', fumble_description): 
                    quarterback = 'M.Ryan'
                else:
                    print "Problem: (fumble area pass) " + str(fumble_description)


                if re.search(r' INTERCEPTED ', fumble_description): #fumble attributed to receiver
                    receiver_string = fumble_description[fumble_description.find('for')+4:]
                else:
                    receiver_string = fumble_description[fumble_description.find('to')+3:]
                
                receiver_string = receiver_string.replace(" ", "")
                receiver_check = re.search(r'[A-Z][a-z]*\.[A-Z\w]*',receiver_string)

                if receiver_check:
                    receiver = receiver_check.group()
                    if (receiver,gameid,team) not in unique_gameinfo_receiver_fumble:
                        unique_gameinfo_receiver_fumble[(receiver,gameid,team)] = 1
                    else:
                        unique_gameinfo_receiver_fumble[(receiver,gameid,team)] = unique_gameinfo_receiver_fumble[(receiver,gameid,team)] + 1
                else:
                    if (quarterback,gameid,team) not in unique_gameinfo_qb_fumble:
                        unique_gameinfo_qb_fumble[(quarterback,gameid,team)] = 1
                    else:
                        unique_gameinfo_qb_fumble[(quarterback,gameid,team)] = unique_gameinfo_qb_fumble[(quarterback,gameid,team)] + 1


            elif re.search(r' sacked ',fumble_description): #fumble due to sacking = qb at fault
                quarterback_string = fumble_description[:fumble_description.find(' sacked ')]

                if re.search(r'(Shotgun)',quarterback_string):
                    quarterback_string = quarterback_string[quarterback_string.find('(Shotgun) ')+10:]

                quarterback_string = quarterback_string.replace(" ", "")
                if re.search(r'[A-Z][a-z]*\.[A-Z\w]*',quarterback_string):
                    quarterback = re.search(r'[A-Z][a-z]*\.[A-Z\w]*',quarterback_string).group()
                elif re.search(r'Alex Smith', fumble_description):           
                    quarterback = 'A.Smith'
                elif re.search(r' Ryan ', fumble_description): 
                    quarterback = 'M.Ryan'
                else:
                    print "Problem: (fumble area sacked) " + str(fumble_description)

                if (quarterback,gameid,team) not in unique_gameinfo_qb_fumble:
                    unique_gameinfo_qb_fumble[(quarterback,gameid,team)] = 1
                else:
                    unique_gameinfo_qb_fumble[(quarterback,gameid,team)] = unique_gameinfo_qb_fumble[(quarterback,gameid,team)] + 1

            elif re.search(r' scrambles ',fumble_description):
                quarterback_string = fumble_description[:fumble_description.find(' scrambles ')]

                if re.search(r'(Shotgun)',quarterback_string):
                    quarterback_string = quarterback_string[quarterback_string.find('(Shotgun) ')+10:]

                quarterback_string = quarterback_string.replace(" ", "")
                if re.search(r'[A-Z][a-z]*\.[A-Z\w]*',quarterback_string):
                    quarterback = re.search(r'[A-Z][a-z]*\.[A-Z\w]*',quarterback_string).group()
                elif re.search(r'Alex Smith', fumble_description):           
                    quarterback = 'A.Smith'
                elif re.search(r' Ryan ', fumble_description): 
                    quarterback = 'M.Ryan'
                else:
                    print "Problem: (fumble area scramble)" + str(fumble_description)

                if (quarterback,gameid,team) not in unique_gameinfo_qb_fumble:
                    unique_gameinfo_qb_fumble[(quarterback,gameid,team)] = 1
                else:
                    unique_gameinfo_qb_fumble[(quarterback,gameid,team)] = unique_gameinfo_qb_fumble[(quarterback,gameid,team)] + 1

            #fumble related to the RB
            elif re.search (r' for ',fumble_description) and not re.search(r' pass', fumble_description) and not (re.search(r'REVERSED',fumble_description)
                                                   or re.search(r'No Play',fumble_description)
                                                   or re.search(r' kick', fumble_description)
                                                   or re.search(r' extra point ', fumble_description)
                                                   or re.search(r' punt', fumble_description)
                                                   or re.search(r' field goal', fumble_description)
                                                   or re.search(r' sacked ', fumble_description)
                                                   or re.search(r' kneels ', fumble_description)
                                                   or re.search(r'TWO-POINT ', fumble_description)
                                                   or re.search(r' spiked ', fumble_description)
                                                   or re.search(r'ateral ', fumble_description)
                                                   or re.search(r' in at QB', fumble_description)
                                                   or re.search(r'PENALTY', fumble_description)
                                                   or re.search(r'Penalty', fumble_description)):

                if re.search(r' left ', fumble_description):
                    RB_string = fumble_description[:fumble_description.find(' left ')]
                elif re.search(r' right ', fumble_description):
                    RB_string = fumble_description[:fumble_description.find(' right ')]
                elif re.search(r' up ', fumble_description):
                    RB_string = fumble_description[:fumble_description.find(' up ')]
                elif re.search(r' to ', fumble_description):
                    RB_string = fumble_description[:fumble_description.find(' to ')]
                elif re.search(r' pushed ', fumble_description):
                    RB_string = fumble_description[:fumble_description.find(' pushed ')]
                elif re.search(r' ran ', fumble_description):
                    RB_string = fumble_description[:fumble_description.find(' ran ')]
                elif re.search(r' rushed ',fumble_description):
                    RB_string = fumble_description[:fumble_description.find(' rushed ')]
                    if re.search(r'Warrick Dunn',RB_string):
                        RB_string = 'W.Dunn'
                    elif re.search(r'Michael Vick',RB_string):
                        RB_string = 'M.Vick'
                    elif re.search(r'Curtis Martin',RB_string):
                        RB_string = 'C.Martin'
                    elif re.search(r'Justin Griffith',RB_string):
                        RB_string = 'J.Griffith'
                    elif re.search(r'Brooks Bollinger',RB_string):
                        RB_string = 'B.Bollinger'
                    elif re.search(r'Kevin Smith',RB_string):
                        RB_string = 'K.Smith'
                    elif re.search(r'Alex Smith',RB_string):
                        RB_string = 'A.Smith'
                    elif re.search(r'Pierre Thomas',RB_string):
                        RB_string = 'P.Thomas'
                    elif re.search(r'Mark Ingram',RB_string):
                        RB_string = 'M.Ingram'
                    elif re.search(r'Titus Young',RB_string):
                        RB_string = 'T.Young'
                    elif re.search(r'Stefan Logan',RB_string):
                        RB_string = 'S.Logan'
                    elif re.search(r'Maurice Morris',RB_string):
                        RB_string = 'M.Morris'
                    elif re.search(r'Darren Sproles',RB_string):
                        RB_string = 'D.Sproles'
                    elif re.search(r'Robert Meachem',RB_string):
                        RB_string = 'R.Meachem'
                    elif re.search(r'Keiland Williams',RB_string):
                        RB_string = 'K.Williams'
                    elif re.search(r'Mike Cox',RB_string):
                        RB_string = 'M.Cox'
                    elif re.search(r'Anthony Dixon',RB_string):
                        RB_string = 'A.Dixon'
                    elif re.search(r'LaMichael James',RB_string):
                        RB_string = 'L.James'
                    elif re.search(r'Michael Turner',RB_string):
                        RB_string = 'M.Turner'
                    elif re.search(r'Jacquizz Rodgers',RB_string):
                        RB_string = 'J.Rodgers'
                    elif re.search(r'Frank Gore',RB_string):
                        RB_string = 'F.Gore'
                    elif re.search(r'Colin Kaepernick',RB_string):
                        RB_string = 'C.Kaepernick'
                    elif re.search(r'Jason Snelling',RB_string):
                        RB_string = 'J.Snelling'
                    elif re.search(r'Matt Ryan',RB_string):
                        RB_string = 'M.Ryan'

                elif re.search(r' for ',fumble_description):
                    RB_string = fumble_description[:fumble_description.find(' for ')]
                else:
                    print "PROBLEM..NO RB fumble: " + fumble_description

                if RB_string:
                    if re.search(r'Kevin Smith',RB_string):
                        RB_string = 'K.Smith'
                    RB_string = RB_string.replace(" ", "")
                    if re.search(r'[A-Z][a-z]*\.[A-Z\w]*',RB_string):
                        RB = re.search(r'[A-Z][a-z]*\.[A-Z\w]*',RB_string).group()
                    else:
                        print "Problem: (fumble area NO RB string found rushed)" + RB_string

                else:
                    print "Problem: (fumble area NO RB found rushed)" + str(description)

                if (RB,gameid,team) not in unique_gameinfo_RB_fumble:
                    unique_gameinfo_RB_fumble[(RB,gameid,team)] = 1
                else:
                    unique_gameinfo_RB_fumble[(RB,gameid,team)] = unique_gameinfo_RB_fumble[(RB,gameid,team)] + 1

            #handles cases of <name> FUMBLES i.e. J.Flacco FUMBLES             
            elif not (re.search(r'REVERSED',fumble_description)
                                                   or re.search(r'No Play',fumble_description)
                                                   or re.search(r' kick', fumble_description)
                                                   or re.search(r' extra point ', fumble_description)
                                                   or re.search(r' punt', fumble_description)
                                                   or re.search(r' field goal', fumble_description)
                                                   or re.search(r' sacked ', fumble_description)
                                                   or re.search(r' kneels ', fumble_description)
                                                   or re.search(r'TWO-POINT ', fumble_description)
                                                   or re.search(r' spiked ', fumble_description)
                                                   or re.search(r'ateral ', fumble_description)
                                                   or re.search(r' in at QB', fumble_description)
                                                   or re.search(r'PENALTY', fumble_description)
                                                   or re.search(r'Penalty', fumble_description)):

                if re.search(r'(Shotgun)',fumble_description):
                    quarterback_string = fumble_description[fumble_description.find('(Shotgun) ')+10:]

                quarterback_string = fumble_description.replace(" ", "")
                if re.search(r'[A-Z][a-z]*\.[A-Z\w]*',quarterback_string):
                    quarterback = re.search(r'[A-Z][a-z]*\.[A-Z\w]*',quarterback_string).group()
                elif re.search(r'Alex Smith', fumble_description):           
                    quarterback = 'A.Smith'
                elif re.search(r' Ryan ', fumble_description): 
                    quarterback = 'M.Ryan'
                else:
                    print "Problem: " + str(fumble_description)

                if (quarterback,gameid,team) not in unique_gameinfo_qb_fumble:
                    unique_gameinfo_qb_fumble[(quarterback,gameid,team)] = 1
                else:
                    unique_gameinfo_qb_fumble[(quarterback,gameid,team)] = unique_gameinfo_qb_fumble[(quarterback,gameid,team)] + 1


        #now onto quarterback sacked info
        elif re.search(r' sacked ', description):
            quarterback_string = description[:description.find(' sacked ')]

            if re.search(r'(Shotgun)',quarterback_string):
                quarterback_string = quarterback_string[quarterback_string.find('(Shotgun) ')+10:]

            quarterback_string = quarterback_string.replace(" ", "")
            if re.search(r'[A-Z][a-z]*\.[A-Z\w]*',quarterback_string):
                quarterback = re.search(r'[A-Z][a-z]*\.[A-Z\w]*',quarterback_string).group()
            elif re.search(r'Alex Smith', description):           
                quarterback = 'A.Smith'
            elif re.search(r' Ryan ', description): 
                quarterback = 'M.Ryan'
            else:
                print "Problem (NO QB during sacked portion): " + str(description)  

            sacked_description = description[description.find(' sacked '):]
            sacked_pass_info_set = sacked_description.split('for ')

            if re.search(r' yard', sacked_description) and re.search(r'\d yard', sacked_description):
                if re.search(r'a loss of ', sacked_pass_info_set[1]):
                    sacked_pass_info_set= sacked_pass_info_set[1].split('a loss of ')

                sacked_pass_info = sacked_pass_info_set[1]
                sacked_yds = int(sacked_pass_info.split('yard')[0])
            else:
                sacked_yds = 0
                print 'Problem!!!!!: (no yards mention in sack)' + unclean_description, sacked_pass_info_set

            if (quarterback,gameid,team) not in unique_gameinfo_qb:
                unique_gameinfo_qb[(quarterback,gameid,team)] = np.array([0,0,0,0,0,1,sacked_yds])
            else:
                old_value_qb = unique_gameinfo_qb[(quarterback,gameid,team)]
                unique_gameinfo_qb[(quarterback,gameid,team)] = old_value_qb + np.array([0,0,0,0,0,1,sacked_yds])

            unique_gameinfo_def[(opp_team,gameid)]  = unique_gameinfo_def[(opp_team,gameid)] + np.array([0,0,0,0,1,sacked_yds,0])


        #entering QB and receiver info (if exists)
        elif re.search(r' pass ', description) and not (re.search(r'REVERSED',description)
                                                   or re.search(r'No Play',description)
                                                   or re.search(r'FUMBLE', description) 
                                                   or re.search(r' kick', description)
                                                   or re.search(r' extra point ', description)
                                                   or re.search(r' punt', description)
                                                   or re.search(r' field goal', description)
                                                   or re.search(r' sacked ', description)
                                                   or re.search(r' kneels ', description)
                                                   or re.search(r'TWO-POINT ', description)
                                                   or re.search(r' spiked ', description)
                                                   or re.search(r'ateral ', description)
                                                   or re.search(r' in at QB', description)
                                                   or re.search(r'PENALTY', description)
                                                   or re.search(r'Penalty', description)
                                                   or re.search(r'pass rushes', description)
                                                   or re.search(r'passer ', description)):


            quarterback_string = description[:description.find(' pass ')]

            if re.search(r'(Shotgun)',quarterback_string):
                quarterback_string = quarterback_string[quarterback_string.find('(Shotgun) ')+10:]

            quarterback_string = quarterback_string.replace(" ", "")
            if re.search(r'[A-Z][a-z]*\.[A-Z\w]*',quarterback_string):
                quarterback = re.search(r'[A-Z][a-z]*\.[A-Z\w]*',quarterback_string).group()
            elif re.search(r'Alex Smith', description):           
                quarterback = 'A.Smith'
            elif re.search(r' Ryan ', description): 
                quarterback = 'M.Ryan'
            else:
                print "Problem: QB pass portion " + str(unclean_description)

            new_game_add = np.array([1,0,0,0,0,0,0])

            #let's put in qb info
            if re.search(r' INTERCEPTED',description): 
                new_game_add = new_game_add+np.array([0,0,0,0,1,0,0])
                #[(# of fumbles, tot_pass_yds , tot_rushing_yds, # INT ,# sacks, # sack yds, # pts allowed)]
                unique_gameinfo_def[(opp_team,gameid)]  = unique_gameinfo_def[(opp_team,gameid)] + np.array([0,0,0,1,0,0,0])
            elif re.search(r'pass incomplete', description):
                count_incomplete = 0
            elif not re.search(r'no gain',description)             and not re.search(r'Lateral',description) and not re.search(r'ackward',description):
                description = description[description.find(' pass '):]
                description = description[description.find(' to '):]
                pass_info_set = description.split('for ')

                if re.search(r'TOUCHDOWN', description): 
                    new_game_add = new_game_add+np.array([0,0,0,1,0,0,0])

                if re.search(r' yard', description):
                    pass_info = pass_info_set[1]
                    yds = int(pass_info.split('yard')[0])
                else:
                    yds = 0
                    print 'Problem!!!!!: no yds in qb/receiver area ' + unclean_description, pass_info_set


                new_game_add = new_game_add+ np.array([0,1,yds,0,0,0,0])
                #Defense: [(# of fumbles, tot_pass_yds , tot_rushing_yds, # INT ,# sacks, # sack yds, # pts allowed)]
                unique_gameinfo_def[(opp_team,gameid)]  = unique_gameinfo_def[(opp_team,gameid)] + np.array([0,yds,0,0,0,0,0])

            elif re.search(r'no gain',description):
                new_game_add = new_game_add+ np.array([0,1,0,0,0,0,0])
            else:
                count_special_cases = count_special_cases + 1
                print "SPECIAL CASE:: qb/receiver" + description


            #if first time, then initialize game info array [(ATT , COMP , YDS , TD , INT )]
            if re.search(r' INTERCEPTED ', description):
                receiver_string = description[description.find('for')+4:]
            else:
                receiver_string = description[description.find('to')+3:]
                
            receiver_string = receiver_string.replace(" ", "")
            receiver_check = re.search(r'[A-Z][a-z]*\.[A-Z\w]*',receiver_string)

            if receiver_check:
                receiver = receiver_check.group()

                #Add in info for receiver
                if (receiver,gameid,team) not in unique_gameinfo_receiver:
                    unique_gameinfo_receiver[(receiver,gameid,team)] = no_value

                old_value_receiver = unique_gameinfo_receiver[(receiver,gameid,team)]
                unique_gameinfo_receiver[(receiver,gameid,team)] = old_value_receiver + new_game_add

            #Add in info for quarterback
            if (quarterback,gameid,team) not in unique_gameinfo_qb:
                unique_gameinfo_qb[(quarterback,gameid,team)] = no_value

            old_value_qb = unique_gameinfo_qb[(quarterback,gameid,team)]
            unique_gameinfo_qb[(quarterback,gameid,team)] = old_value_qb + new_game_add


        #Running back info    
        elif re.search (r' for ',description) and not re.search(r' pass', description) and not (re.search(r'REVERSED',description)
                                                   or re.search(r'No Play',description)
                                                   or re.search(r'FUMBLE', description) 
                                                   or re.search(r' kick', description)
                                                   or re.search(r' extra point ', description)
                                                   or re.search(r' punt', description)
                                                   or re.search(r' field goal', description)
                                                   or re.search(r' sacked ', description)
                                                   or re.search(r' kneels ', description)
                                                   or re.search(r'TWO-POINT ', description)
                                                   or re.search(r' spiked ', description)
                                                   or re.search(r'ateral ', description)
                                                   or re.search(r' in at QB', description)
                                                   or re.search(r'PENALTY', description)
                                                   or re.search(r'Penalty', description)) :





            if re.search(r' up ', description):
                RB_string = description[:description.find(' up ')]
            elif re.search(r' left ', description):
                RB_string = description[:description.find(' left ')]
            elif re.search(r' right ', description):
                RB_string = description[:description.find(' right ')]
            elif re.search(r' to ', description):
                RB_string = description[:description.find(' to ')]
            elif re.search(r' pushed ', description):
                RB_string = description[:description.find(' pushed ')]
            elif re.search(r' ran ', description):
                RB_string = description[:description.find(' ran ')]
            elif re.search(r' rushed ',description):            
                RB_string = description[:description.find(' rushed ')]
                if re.search(r'Warrick Dunn',RB_string):
                    RB_string = 'W.Dunn'
                elif re.search(r'Michael Vick',RB_string):
                    RB_string = 'M.Vick'
                elif re.search(r'Curtis Martin',RB_string):
                    RB_string = 'C.Martin'
                elif re.search(r'Justin Griffith',RB_string):
                    RB_string = 'J.Griffith'
                elif re.search(r'Brooks Bollinger',RB_string):
                    RB_string = 'B.Bollinger'
                elif re.search(r'Kevin Smith',RB_string):
                    RB_string = 'K.Smith'
                elif re.search(r'Alex Smith',RB_string):
                    RB_string = 'A.Smith'
                elif re.search(r'Pierre Thomas',RB_string):
                    RB_string = 'P.Thomas'
                elif re.search(r'Mark Ingram',RB_string):
                    RB_string = 'M.Ingram'
                elif re.search(r'Titus Young',RB_string):
                    RB_string = 'T.Young'
                elif re.search(r'Stefan Logan',RB_string):
                    RB_string = 'S.Logan'
                elif re.search(r'Maurice Morris',RB_string):
                    RB_string = 'M.Morris'
                elif re.search(r'Darren Sproles',RB_string):
                    RB_string = 'D.Sproles'
                elif re.search(r'Robert Meachem',RB_string):
                    RB_string = 'R.Meachem'
                elif re.search(r'Keiland Williams',RB_string):
                    RB_string = 'K.Williams'
                elif re.search(r'Mike Cox',RB_string):
                    RB_string = 'M.Cox'
                elif re.search(r'Anthony Dixon',RB_string):
                    RB_string = 'A.Dixon'
                elif re.search(r'LaMichael James',RB_string):
                    RB_string = 'L.James'
                elif re.search(r'Michael Turner',RB_string):
                    RB_string = 'M.Turner'
                elif re.search(r'Jacquizz Rodgers',RB_string):
                    RB_string = 'J.Rodgers'
                elif re.search(r'Frank Gore',RB_string):
                    RB_string = 'F.Gore'
                elif re.search(r'Colin Kaepernick',RB_string):
                    RB_string = 'C.Kaepernick'
                elif re.search(r'Jason Snelling',RB_string):
                    RB_string = 'J.Snelling'
                elif re.search(r'Matt Ryan',RB_string):
                    RB_string = 'M.Ryan'

            elif re.search(r' for ', description):
                RB_string = description[:description.find(' for ')]
            else:
                print "PROBLEM..NO RB: (rb info)" + description

            if RB_string:
                if re.search(r'Kevin Smith',RB_string):
                    RB_string = 'K.Smith'
                elif re.search(r'Alex Smith',RB_string):
                    RB_string = 'A.Smith'
                RB_string = RB_string.replace(" ", "")
                if re.search(r'[A-Z][a-z]*\.[A-Z\w]*',RB_string):
                    RB = re.search(r'[A-Z][a-z]*\.[A-Z\w]*',RB_string).group()
                else:
                    print "Problem: no rb in rb string " + unclean_description
            else:
                print "Problem: no rb in rushed " + str(description)

            new_game_add_RB = np.array([0,0,0,0,0,0])

            if re.search(r' middle ',description): 
                new_game_add_RB = new_game_add_RB+np.array([1,0,0,0,0,0])        
            if re.search(r' left ', description):
                new_game_add_RB = new_game_add_RB+np.array([0,1,0,0,0,0])    
            if re.search(r' right ', description):
                new_game_add_RB = new_game_add_RB+np.array([0,0,1,0,0,0])

            if re.search(r' for no gain', description):
                yds = 0
            elif re.search(r' for \d', description) or re.search(r' for -\d', description) :
                for split_info in description.split(' for '):
                    if re.search(r'\d yard', split_info) and (re.search(r'^\d', split_info) or re.search(r'^-\d', split_info)):
                        yds = int(split_info.split('yard')[0])

                if re.search(r'TOUCHDOWN', description) or re.search(r'touchdown', description): 
                    new_game_add_RB = new_game_add_RB+np.array([0,0,0,0,1,0])

                new_game_add_RB = new_game_add_RB+ np.array([0,0,0,yds,0,0])

                unique_gameinfo_def[(opp_team,gameid)]  = unique_gameinfo_def[(opp_team,gameid)] + np.array([0,0,yds,0,0,0,0])

            if (RB,gameid,team) not in unique_gameinfo_RB:
                unique_gameinfo_RB[(RB,gameid,team)] = np.array([0,0,0,0,0,0])

            old_value_RB = unique_gameinfo_RB[(RB,gameid,team)]
            unique_gameinfo_RB[(RB,gameid,team)] = old_value_RB + new_game_add_RB


    #making the variables for the table for quarterback

    count_noqb = 0

    date_all = []
    qb_all = []
    qbr_all = []
    att_all = []
    comp_all = []
    yds_all = []
    td_all = []
    inter_all = []
    sacks_all = []
    sack_yds_all = []
    rush_all = []
    rush_yds_all = []
    score_all = []
    fumble_all = []
    opp_score_all = []
    win_all = []
    tie_all = []
    team_all = []
    game_id_all = []
    season_all = []
    opp_team = []
    for gameinfo in unique_gameinfo_qb:


        quarterback = gameinfo[0]
        date_played = gameinfo[1][0:gameinfo[1].find('_')]
        game_name = gameinfo[1][gameinfo[1].find('_')+1:]
        team = gameinfo[2]


        data_game = unique_gameinfo_qb[gameinfo]

        value = calc_qb_rating(data_game[0] , data_game[1] , data_game[2] , data_game[3] , data_game[4])

        if data_game[0] > 10: #qb must have at least 10 passes
            td = 0
            fumbles = 0

            if gameinfo in unique_gameinfo_qb_fumble:
                fumbles = unique_gameinfo_qb_fumble[gameinfo]

            if gameinfo in unique_gameinfo_RB_fumble:
                fumbles = fumbles + unique_gameinfo_RB_fumble[gameinfo]

            fumble_all.append(fumbles)

            if gameinfo in unique_gameinfo_RB: #rushing info for QB
                data_game_RB = unique_gameinfo_RB[gameinfo]
                rush_all.append(data_game_RB[0]+data_game_RB[1]+data_game_RB[2])
                rush_yds_all.append(data_game_RB[3])
                td = data_game_RB[4]
            else:
                rush_all.append(0)
                rush_yds_all.append(0)          

            #pretty terrible code: let's start to add pandas here
            date_all.append(date_played)
            qb_all.append(quarterback)
            team_all.append(team)
            game_id_all.append(gameinfo[1])
            qbr_all.append(value)
            att_all.append(data_game[0])
            comp_all.append(data_game[1])
            yds_all.append(data_game[2])
            td_all.append(data_game[3]+td)
            inter_all.append(data_game[4])
            sacks_all.append(data_game[5])
            sack_yds_all.append(data_game[6])
            season_all.append(year)

            #adding game scores here
            game_scores = unique_game_scores[gameinfo[1]]

            if game_scores[4] == 1:
                score_all.append(game_scores[2])
                opp_score_all.append(game_scores[3])
                win_all.append(0)
                tie_all.append(1)
            else:
                tie_all.append(0)

                if gameinfo[2] == game_scores[0]: #winner winner chicken dinner
                    score_all.append(game_scores[2])
                    opp_score_all.append(game_scores[3])
                    win_all.append(1)
                else:  #loser
                    score_all.append(game_scores[3])
                    opp_score_all.append(game_scores[2])
                    win_all.append(0)

            if team != game_scores[0]:
                opp_team.append(game_scores[0])
            else:
                opp_team.append(game_scores[1])

        else:
            count_noqb = count_noqb + 1

    print len(date_all)
    print "Making data frame"

    #panda dataframe and import to sql
    #make the panda dataframe
    data_qb = {'DatePlayed': date_all,
            'QB': qb_all,
            'Team': team_all,
            'OppTeam':opp_team,
            'GameID': game_id_all,   
            'QBRating': qbr_all ,
            'Attempts': att_all,
            'Completions': comp_all,
            'TotYds': yds_all,
            'TotTD': td_all,
            'TotInterception': inter_all,
            'TotSacks': sacks_all,
            'SackYds': sack_yds_all,
            'TotRush': rush_all,
            'RushYds': rush_yds_all,
            'Fumbles': fumble_all,
            'Score': score_all,
            'OppScore': opp_score_all,
            'Win': win_all,
            'Tie': tie_all,
            'Season': season_all}

    qb_frame = pd.DataFrame(data_qb,columns=['DatePlayed','QB','Team','OppTeam','GameID','QBRating',                                             'Attempts','Completions','TotYds','TotTD','TotInterception',                                              'TotSacks','SackYds','TotRush','RushYds','Fumbles','Score',                                              'OppScore','Win','Tie','Season'], dtype =float)

    print "Importing QB to sql"

    engine = create_engine("mysql+pymysql://root:@localhost/qb")
    qb_frame_dtypes = {'DatePlayed':int(11)}
#    qb_frame.to_sql('qb_2002_2012', engine, index=False, if_exists='append',dtype={'DatePlayed':DOUBLE})

    print "DONE with QB"

    #Time for RB info:  below is the structure
    #unique_gameinfo_RB = {} #[middle,left,right,yds,td, nothing]
    count_noRB = 0
    date_all = []
    RB_all = []
    att_all = []
    yds_all = []
    pass_yds_all = []
    pass_attempts_all = []
    inter_all =[]
    td_all = []
    score_all = []
    fumble_all = []
    opp_score_all = []
    win_all = []
    tie_all = []
    team_all = []
    opp_team = []
    game_id_all = []
    season_all = []

    for gameinfo in unique_gameinfo_RB:
        RB = gameinfo[0]
        date_played = gameinfo[1][0:gameinfo[1].find('_')]
        game_name = gameinfo[1][gameinfo[1].find('_')+1:]
        team = gameinfo[2]

        data_game = unique_gameinfo_RB[gameinfo]

        if data_game[0]+data_game[1]+data_game[2] > 10 and gameinfo not in unique_gameinfo_qb: #RB must have 10 attempts and not a qb

            fumbles = 0
            if gameinfo in unique_gameinfo_receiver_fumble:
                fumbles = unique_gameinfo_receiver_fumble[gameinfo]
            if gameinfo in unique_gameinfo_RB_fumble:
                fumble_all.append(fumbles + unique_gameinfo_RB_fumble[gameinfo])
            else:
                fumble_all.append(fumbles)

            #pretty terrible code: let's start to add pandas here
            date_all.append(date_played)
            RB_all.append(RB)
            team_all.append(team)
            game_id_all.append(gameinfo[1])
            att_all.append(data_game[0]+data_game[1]+data_game[2])
            yds_all.append(data_game[3])
            season_all.append(year)
            
            td = data_game[4]

            if gameinfo in unique_gameinfo_receiver:
                data_game_receiver = unique_gameinfo_receiver[gameinfo]
                inter_all.append(data_game_receiver[4])
                td = td+data_game_receiver[3]
                pass_yds_all.append(data_game_receiver[2])
                pass_attempts_all.append(data_game_receiver[0])
            else:
                pass_yds_all.append(0)
                pass_attempts_all.append(0)
                inter_all.append(0)

            td_all.append(td)

            #let's add the score info here
            game_scores = unique_game_scores[gameinfo[1]]
            if game_scores[4] == 1:
                score_all.append(game_scores[2])
                opp_score_all.append(game_scores[3])
                win_all.append(0)
                tie_all.append(1)
            else:
                tie_all.append(0)

                if gameinfo[2] == game_scores[0]: #winner winner chicken dinner
                    score_all.append(game_scores[2])
                    opp_score_all.append(game_scores[3])
                    win_all.append(1)
                else:  #loser
                    score_all.append(game_scores[3])
                    opp_score_all.append(game_scores[2])
                    win_all.append(0)

            if team != game_scores[0]:
                opp_team.append(game_scores[0])
            else:
                opp_team.append(game_scores[1])
           


        else:
            count_noRB = count_noRB + 1

    print "Making data frame"

    #panda dataframe and import to sql
    #make the panda dataframe
    data_RB = {'DatePlayed': date_all,
            'RB': RB_all,
            'Team': team_all,
            'OppTeam': opp_team,
            'GameID': game_id_all,   
            'Attempts': att_all,
            'TotYds': yds_all,
            'TotPassYds': pass_yds_all,
            'PassAttempts': pass_attempts_all,
            'TotInterception': inter_all,
            'TotTD': td_all,
            'Fumbles': fumble_all,
            'Score': score_all,
            'OppScore': opp_score_all,
            'Win': win_all,
            'Tie': tie_all,
            'Season': season_all}

    RB_frame = pd.DataFrame(data_RB,columns=['DatePlayed','RB','Team','OppTeam','GameID',                                             'Attempts','TotYds','TotTD', 'TotPassYds','PassAttempts',                                             'Fumbles','Score','TotInterception',                                              'OppScore','Win','Tie','Season'], dtype =float)


    print "Importing RB to sql"
    engine = create_engine("mysql+pymysql://root:@localhost/RB")
    RB_frame_dtypes = {'DatePlayed':int(11)}
    #RB_frame.to_sql('RB_2002_2012', engine, index=False, if_exists='append',dtype={'DatePlayed':DOUBLE})

    print "DONE with RB"


    #making the variables for the table for receiver
    #unique_gameinfo_receiver = {} #[(ATT , COMP , YDS , TD , INT ,# sacks, # sack yds)]

    count_noreceiver = 0

    date_all = []
    receiver_all = []
    att_all = []
    comp_all = []
    yds_all = []
    td_all = []
    inter_all = []
    score_all = []
    fumble_all = []
    opp_score_all = []
    win_all = []
    tie_all = []
    team_all = []
    game_id_all = []
    season_all = []
    opp_team = []
    #let's make unique RB list:
    RB_to_check = np.unique(RB_frame.RB[:])
    
    for gameinfo in unique_gameinfo_receiver:


        receiver = gameinfo[0]
        date_played = gameinfo[1][0:gameinfo[1].find('_')]
        game_name = gameinfo[1][gameinfo[1].find('_')+1:]
        team = gameinfo[2]

        data_game = unique_gameinfo_receiver[gameinfo]

        if data_game[0] > 3: #158.3:
            if receiver not in RB_to_check:
                if gameinfo in unique_gameinfo_receiver_fumble:
                    fumble_all.append(unique_gameinfo_receiver_fumble[gameinfo])
                else:
                    fumble_all.append(0)

                #unique_gameinfo_receiver = {} #[(ATT , COMP , YDS , TD , INT ,# sacks, # sack yds)]
                #pretty terrible code: let's start to add pandas here
                date_all.append(date_played)
                receiver_all.append(receiver)
                team_all.append(team)
                game_id_all.append(gameinfo[1])
                att_all.append(data_game[0])
                comp_all.append(data_game[1])
                yds_all.append(data_game[2])
                td_all.append(data_game[3])
                inter_all.append(data_game[4])
                season_all.append(year)
 
               #let's add the score info here
                #unique_game_scores = [winning team, losing team, winning score, losing score, tie = 1 and no tie = 0]
                game_scores = unique_game_scores[gameinfo[1]]
                if game_scores[4] == 1:
                    score_all.append(game_scores[2])
                    opp_score_all.append(game_scores[3])
                    win_all.append(0)
                    tie_all.append(1)
                else:
                    tie_all.append(0)

                    if gameinfo[2] == game_scores[0]: #winner winner chicken dinner
                        score_all.append(game_scores[2])
                        opp_score_all.append(game_scores[3])
                        win_all.append(1)
                    else:  #loser
                        score_all.append(game_scores[3])
                        opp_score_all.append(game_scores[2])
                        win_all.append(0)
                if team != game_scores[0]:
                    opp_team.append(game_scores[0])
                else:
                    opp_team.append(game_scores[1])
        else:
            count_noreceiver = count_noreceiver + 1

    print "Making data frame"

    #panda dataframe and import to sql
    #make the panda dataframe
    data_receiver = {'DatePlayed': date_all,
            'Receiver': receiver_all,
            'Team': team_all,
            'OppTeam': opp_team,
            'GameID': game_id_all,   
            'Attempts': att_all,
            'Completions': comp_all,
            'TotYds': yds_all,
            'TotTD': td_all,
            'TotInterception': inter_all,
            'Fumbles': fumble_all,
            'Score': score_all,
            'OppScore': opp_score_all,
            'Win': win_all,
            'Tie': tie_all,
            'Season': season_all}

    receiver_frame = pd.DataFrame(data_receiver,columns=['DatePlayed','Receiver','Team','OppTeam','GameID',                                             'Attempts','Completions','TotYds','TotTD','TotInterception',                                              'Fumbles','Score',                                              'OppScore','Win','Tie','Season'], dtype =float)

    print "Importing Receiver to sql"
    engine = create_engine("mysql+pymysql://root:@localhost/receiver")
    receiver_frame_dtypes = {'DatePlayed':int(11)}
    #receiver_frame.to_sql('receiver_2002_2012', engine, index=False, if_exists='append',dtype={'DatePlayed':DOUBLE})
    print "DONE with Receiver"

    #moving onto defense
    #unique_gameinfo_def = {} #[(# of fumbles, tot_pass_yds , tot_rushing_yds, # INT ,# sacks, # sack yds, # pts allowed)]

    count_nodef = 0

    date_all = []
    def_all = []
    fumble_all = []
    tot_pass_all = []
    tot_rush_all = []
    inter_all = []
    sacks_all = []
    sack_yds_all = []
    points_all = []
    score_all = []
    opp_score_all = []
    win_all = []
    tie_all = []
    team_all = []
    game_id_all = []
    season_all = []
    opp_team = []
    
    for gameinfo in unique_gameinfo_def:
        defense = gameinfo[0]
        date_played = gameinfo[1][0:gameinfo[1].find('_')]
        game_name = gameinfo[1][gameinfo[1].find('_')+1:]

        data_game = unique_gameinfo_def[gameinfo]

        date_all.append(date_played)
        def_all.append(defense)
        game_id_all.append(gameinfo[1])

        # Data structure[(# of fumbles, tot_pass_yds , tot_rushing_yds, # INT ,# sacks, # sack yds, # pts allowed)]
        fumble_all.append(data_game[0])
        tot_pass_all.append(data_game[1])
        tot_rush_all.append(data_game[2])
        inter_all.append(data_game[3])
        sacks_all.append(data_game[4])
        sack_yds_all.append(data_game[5])
        points_all.append(data_game[6])
        season_all.append(year)

        game_scores = unique_game_scores[gameinfo[1]]
        if game_scores[4] == 1:
            score_all.append(game_scores[2])
            opp_score_all.append(game_scores[3])
            win_all.append(0)
            tie_all.append(1)
        else:
            tie_all.append(0)

            if defense == game_scores[0]: #winner winner chicken dinner
                score_all.append(game_scores[2])
                opp_score_all.append(game_scores[3])
                win_all.append(1)
            else:  #loser
                score_all.append(game_scores[3])
                opp_score_all.append(game_scores[2])
                win_all.append(0)

        #opposites since this is talking about the defense of the other team; the mirror image
        if team == game_scores[0]:
            opp_team.append(game_scores[0])
        else:
            opp_team.append(game_scores[1])

    print "Making data frame"

    #panda dataframe and import to sql
    #make the panda dataframe
    data_def = {'DatePlayed': date_all,
            'Defense': def_all,
            'OppTeam': opp_team,
            'GameID': game_id_all,   
            'TotPassYds': tot_pass_all,
            'TotRushYds': tot_rush_all,
            'TotInterception': inter_all,
            'TotSacks': sacks_all,
            'SackYds': sack_yds_all,
            'Fumbles': fumble_all,
            'Score': score_all,
            'OppScore': opp_score_all,
            'Win': win_all,
            'Tie': tie_all,
            'Season': season_all}

    print len(def_all), len(score_all)

    defense_frame = pd.DataFrame(data_def,columns=['DatePlayed','Defense','OppTeam','GameID',                                             'TotPassYds','TotRushYds','TotInterception',                                             'TotSacks','SackYds','Fumbles','Score',                                              'OppScore','Win','Tie','Season'], dtype =float)

    print defense_frame.head()

    print "Importing defense to sql"
    engine = create_engine("mysql+pymysql://root:@localhost/defense")
    defense_frame_dtypes = {'DatePlayed':int(11)}
    #defense_frame.to_sql('defense_2002_2012', engine, index=False, if_exists='append',dtype={'DatePlayed':DOUBLE})

    print "DONE with defense"

print "COMPLETELY DONE!!!"

