# documentation from :
# https://github.com/swar/nba_api/blob/master/docs/table_of_contents.md

import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playbyplayv2,boxscoreadvancedv2,boxscorescoringv2,boxscoretraditionalv2,commonteamroster,shotchartdetail,boxscoreusagev2,gamerotation
import os

def get_nba_teams_and_ids():
    nba_teams = ['MIL', 'CHI', 'CHA', 'TOR', 'BOS', 'PHX', 'OKC', 'LAC', 'IND',
                 'BKN', 'MIN', 'UTA', 'SAS', 'DAL', 'CLE', 'NYK', 'POR', 'HOU',
                 'DEN', 'MEM', 'SAC', 'PHI', 'ATL', 'LAL',
                 'WAS', 'ORL', 'GSW', 'NOP',
                 'MIA',
                 'DET']
    nba_team_ids = [1610612749, 1610612766, 1610612738, 1610612746, 1610612754,
                    1610612750, 1610612741, 1610612742, 1610612762, 1610612759,
                    1610612739, 1610612752, 1610612761, 1610612760, 1610612757,
                    1610612751, 1610612745, 1610612756, 1610612743, 1610612755,
                    1610612737, 1610612763, 1610612764, 1610612744, 1610612740,
                    1610612753, 1610612747, 1610612758, 1610612748, 1610612765]
    return nba_teams,nba_team_ids

def get_nba_games(start_date, end_date):
    # TODO: check for duplicates and make sure they arent inserted twice

    # mm/d/yyyy
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    start_date = start_date.strftime("%m/%d/%Y")
    gamefinder = leaguegamefinder.LeagueGameFinder(date_from_nullable=start_date)
    all_games = gamefinder.get_data_frames()[0]
    all_games['GAME_DATE'] = pd.to_datetime(all_games['GAME_DATE'])
    print('Number of games = ', len(all_games))
    all_games = all_games[(all_games['GAME_DATE'] < end_date)]
    all_games = all_games[all_games['TEAM_ID'].isin(nba_team_ids)]


    try:
        # read existing games that are located in file - loop through them and see which have been parsed
        if os.path.isfile('data/all_games.csv'):
            all_games_read = pd.read_csv('data/all_games.csv')

            for ix,row in all_games.iterrows():

                TEAM_ID   = row['TEAM_ID']
                GAME_ID   = row['GAME_ID']
                SEASON_ID = row['SEASON_ID']
                filtered_game = all_games_read[(all_games_read['GAME_ID']==int(GAME_ID))&
                                  (all_games_read['SEASON_ID']==int(SEASON_ID))&
                                  (all_games_read['TEAM_ID']==int(TEAM_ID))]
                if len(filtered_game)==0:
                    print(TEAM_ID,GAME_ID,SEASON_ID)
                    row.to_csv('data/all_games.csv', header=False, index=False, mode='a')
        else:
            all_games.to_csv( 'data/all_games.csv' , header=True, index=False,mode='a')

    except Exception as e:
        print(e)

    finally:
        return True


def get_play_by_play(game_id):
    # deal with play by play
    pbp = playbyplayv2.PlayByPlayV2(game_id)
    pbp = pbp.get_data_frames()[0]
    if os.path.isfile('data/large_playbyplayv2_df.csv'):
        pbp.to_csv('data/large_playbyplayv2_df.csv',header=False, index=False, mode='a', encoding='utf-8')
    else:
        pbp.to_csv('data/large_playbyplayv2_df.csv',header=True, index=False, mode='a', encoding='utf-8')
    return True

def get_boxscoretraditionalv2(game_id):
    # deal with play by play
    bx = boxscoretraditionalv2.BoxScoreTraditionalV2(game_id)
    bx = bx.get_data_frames()[0]
    if os.path.isfile('data/large_boxscoretraditionalv2_df.csv'):
        bx.to_csv('data/large_boxscoretraditionalv2_df.csv',header=False, index=False, mode='a', encoding='utf-8')
    else:
        bx.to_csv('data/large_boxscoretraditionalv2_df.csv',header=True, index=False, mode='a', encoding='utf-8')
    return True

def get_large_boxscorescoringv2_df(game_id):
    # deal with play by play
    bx = boxscorescoringv2.BoxScoreTraditionalV2(game_id)
    bx = bx.get_data_frames()[0]
    if os.path.isfile('data/large_boxscoretraditionalv2_df.csv'):
        bx.to_csv('data/large_boxscoretraditionalv2_df.csv',header=False, index=False, mode='a', encoding='utf-8')
    else:
        bx.to_csv('data/large_boxscoretraditionalv2_df.csv',header=True, index=False, mode='a', encoding='utf-8')
    return True


def get_boxscoreadvancedv2(game_id):
    # deal with play by play
    bx = boxscoreadvancedv2.BoxScoreAdvancedV2(game_id)
    bx = bx.get_data_frames()[0]
    if os.path.isfile('data/large_boxscoreadvancedv2_df.csv'):
        bx.to_csv('data/large_boxscoreadvancedv2_df.csv',header=False, index=False, mode='a', encoding='utf-8')
    else:
        bx.to_csv('data/large_boxscoreadvancedv2_df.csv',header=True, index=False, mode='a', encoding='utf-8')
    return True

def get_team_rosters(year,team_list):
    print('get_team_rosters -------->')
    do_scrape = False
    # load current roster file - if year exists - return without action
    if os.path.isfile('data/rosters.csv'):
        rosters = pd.read_csv('data/rosters.csv')
        if len(rosters[rosters['SEASON']==year])==0:
            do_scrape = True
    else:
        do_scrape=True

    if do_scrape == True:
        yearly_roster = pd.DataFrame()
        for team in team_list:
            res = commonteamroster.CommonTeamRoster(season=year, team_id=team)
            d = res.get_data_frames()[0]
            yearly_roster = yearly_roster.append(d)
        if os.path.isfile('data/rosters.csv'):
            yearly_roster.to_csv('data/rosters.csv',header=False, index=False, mode='a', encoding='utf-8' )
        else:
            yearly_roster.to_csv('data/rosters.csv', header=True, index=False, mode='a', encoding='utf-8')
    else:
        print('No need to scrape rosters')
    print('get_team_rosters <---------')
    return True

def get_shot_chart_detail(rosters):
    print('get_shot_chart_detail ---->')
    curr_roster = rosters
    curr_roster = curr_roster[['TeamID','PLAYER_ID']]
    shot_chart_appended = pd.DataFrame()
    prog_counter = 1
    for ix, row in curr_roster.iterrows():
        print(float(prog_counter)/float(len(curr_roster)))
        prog_counter +=1
        TeamID = row['TeamID']
        PLAYER_ID = row['PLAYER_ID']
        res = shotchartdetail.ShotChartDetail(player_id=PLAYER_ID, team_id=TeamID)
        d = res.get_data_frames()[0]
        shot_chart_appended = shot_chart_appended.append(d)

    shot_chart_appended.to_csv('shot_chart_using_roster_2021.csv', index=False)
    print('get_shot_chart_detail <----')
    return True

def get_boxscoreusagev2(game_id):
    print('get_boxscoreusagev2 ---->')
    # deal with play by play
    bx = boxscoreusagev2.BoxScoreUsageV2(game_id)
    bx = bx.get_data_frames()[0]
    if os.path.isfile('data/large_boxscoreusagev2.csv'):
        bx.to_csv('data/large_boxscoreusagev2.csv', header=False, index=False, mode='a', encoding='utf-8')
    else:
        bx.to_csv('data/large_boxscoreusagev2.csv', header=True, index=False, mode='a', encoding='utf-8')
    return True

def get_gamerotation(game_id):
    print('get_gamerotation ---->')
    # deal with play by play
    df = gamerotation.GameRotation(game_id)
    bx_home = df.get_data_frames()[0]
    bx_away = df.get_data_frames()[1]
    bx = bx_home.append(bx_away)
    if os.path.isfile('data/large_gamerotation.csv'):
        bx.to_csv('data/large_gamerotation.csv', header=False, index=False, mode='a', encoding='utf-8')
    else:
        bx.to_csv('data/large_gamerotation.csv', header=True, index=False, mode='a', encoding='utf-8')
    return True

if __name__ == "__main__":
    nba_teams,nba_team_ids = get_nba_teams_and_ids()


    from datetime import datetime
    start_date = '2020-12-22'
    end_date   = '2021-06-29'

    # get_nba_games(start_date, end_date)

    all_games = pd.read_csv('data/all_games.csv',dtype={'GAME_ID':str})

    counter = 0
    # if os.path.isfile('data/large_playbyplayv2_df.csv'):
    #     old_play_by_play = pd.read_csv('data/large_playbyplayv2_df.csv',dtype={'GAME_ID':str})
    # else:
    #     old_play_by_play = pd.DataFrame(columns=['GAME_ID'])
    #
    # if os.path.isfile('data/large_boxscoretraditionalv2_df.csv'):
    #     old_boxscoretraditionalv2 = pd.read_csv('data/large_boxscoretraditionalv2_df.csv',dtype={'GAME_ID':str})
    # else:
    #     old_boxscoretraditionalv2 = pd.DataFrame(columns=['GAME_ID'])
    #
    # if os.path.isfile('data/large_boxscoreadvancedv2_df.csv'):
    #     old_large_boxscoreadvancedv2_df = pd.read_csv('data/large_boxscoreadvancedv2_df.csv',dtype={'GAME_ID':str})
    # else:
    #     old_large_boxscoreadvancedv2_df = pd.DataFrame(columns=['GAME_ID'])

    if os.path.isfile('data/large_boxscoreusagev2.csv'):
        old_large_boxscoreusagev2 = pd.read_csv('data/large_boxscoreusagev2.csv',dtype={'GAME_ID':str})
    else:
        old_large_boxscoreusagev2 = pd.DataFrame(columns=['GAME_ID'])

    if os.path.isfile('data/large_gamerotation.csv'):
        old_large_gamerotation = pd.read_csv('data/large_gamerotation.csv',dtype={'GAME_ID':str})
    else:
        old_large_gamerotation = pd.DataFrame(columns=['GAME_ID'])



    for game_id in all_games['GAME_ID'].unique():
        print(game_id, float(counter) / float(len(all_games['GAME_ID'].unique())))
        counter = counter + 1
    #
    #     # play by play
    #     # check if game id exists already
    #     if len(old_play_by_play[old_play_by_play['GAME_ID'] == game_id])==0:
    #         res = get_play_by_play(game_id)
    #     else:
    #         print('PBP: {} already exists'.format(game_id))
    #
    #
    #     # get_boxscoretraditionalv2
    #     if len(old_boxscoretraditionalv2[old_boxscoretraditionalv2['GAME_ID'] == game_id]) == 0:
    #         res =  get_boxscoretraditionalv2(game_id)
    #     else:
    #         print('Box Score traditionalv2 : {} already exists'.format(game_id))
    #
    #     # get_boxscoreadvancedv2
    #     if len(old_large_boxscoreadvancedv2_df[old_large_boxscoreadvancedv2_df['GAME_ID'] == game_id]) == 0:
    #         res =  get_boxscoreadvancedv2(game_id)
    #     else:
    #         print('Box Score traditionalv2 : {} already exists'.format(game_id))

    # get_boxscoreusagev2
        if len(old_large_boxscoreusagev2[old_large_boxscoreusagev2['GAME_ID'] == game_id]) == 0:
            res =  get_boxscoreusagev2(game_id)
        else:
            print('get_boxscoreusagev2 : {} already exists'.format(game_id))

    # get_gamerotation
        if len(old_large_gamerotation[old_large_gamerotation['GAME_ID'] == game_id]) == 0:
            res = get_gamerotation(game_id)
        else:
            print('get_gamerotation : {} already exists'.format(game_id))


    # get team rosters
    # get_team_rosters(2020,nba_team_ids)

    # get shot chart detail by player
    # rosters = pd.read_csv('data/rosters.csv',dtype={'GAME_ID':str , 'PLAYER_ID':str})
    # get_shot_chart_detail(rosters)
print('done')