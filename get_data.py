import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
from nba_api.stats.endpoints import playbyplayv2,boxscoreadvancedv2,boxscorescoringv2,boxscoretraditionalv2
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


if __name__ == "__main__":
    nba_teams,nba_team_ids = get_nba_teams_and_ids()


    from datetime import datetime
    start_date = '2020-12-22'
    end_date   = '2021-06-29'

    get_nba_games(start_date, end_date)

    all_games = pd.read_csv('data/all_games.csv',dtype={'GAME_ID':str})

    counter = 0
    if os.path.isfile('data/large_playbyplayv2_df.csv'):
        old_play_by_play = pd.read_csv('data/large_playbyplayv2_df.csv',dtype={'GAME_ID':str})
    else:
        old_play_by_play = pd.DataFrame(columns=['GAME_ID'])

    if os.path.isfile('data/large_boxscoretraditionalv2_df.csv'):
        old_boxscoretraditionalv2 = pd.read_csv('data/large_boxscoretraditionalv2_df.csv',dtype={'GAME_ID':str})
    else:
        old_boxscoretraditionalv2 = pd.DataFrame(columns=['GAME_ID'])



    for game_id in all_games['GAME_ID'].unique():
        print(game_id, float(counter) / float(len(all_games['GAME_ID'].unique())))
        counter = counter + 1

        # play by play
        # check if game id exists already
        if len(old_play_by_play[old_play_by_play['GAME_ID'] == game_id])==0:
            res = get_play_by_play(game_id)
        else:
            print('PBP: {} already exists'.format(game_id))


        # get_boxscoretraditionalv2
        if len(old_boxscoretraditionalv2[old_boxscoretraditionalv2['GAME_ID'] == game_id]) == 0:
            res =  get_boxscoretraditionalv2(game_id)
        else:
            print('Box Score traditionalv2 : {} already exists'.format(game_id))

print('done')