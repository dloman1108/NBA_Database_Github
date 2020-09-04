import pandas as pd
import sqlalchemy as sa
import os
import yaml
import re
from sklearn.linear_model import LogisticRegression
import pickle

def get_engine(fp):

	yaml_fp=fp[:fp.index('NBA-Projects')]

	if os.path.isfile(yaml_fp+'sql.yaml'):
		with open(yaml_fp+'sql.yaml', 'r') as stream:
			data_loaded = yaml.load(stream)
			
			#domain=data_loaded['SQL_DEV']['domain']
			user=data_loaded['BBALL_STATS']['user']
			password=data_loaded['BBALL_STATS']['password']
			endpoint=data_loaded['BBALL_STATS']['endpoint']
			port=data_loaded['BBALL_STATS']['port']
			database=data_loaded['BBALL_STATS']['database']
			
	db_string = "postgres://{0}:{1}@{2}:{3}/{4}".format(user,password,endpoint,port,database)
	engine=sa.create_engine(db_string)
	
	return engine


def get_data(engine,fp):
	
	win_probability_qry=open(fp+'/win_probability_train.sql').read()
	win_probability_qry=re.sub('%','%%',win_probability_qry)
	win_probability_df=pd.read_sql(win_probability_qry,engine)
	
	print('Data Loaded...')
	return win_probability_df



def build_save_model(wp_df,fp):

	#simple logistic regression model uses score delta, home court status 
	# and time remaining to predict win probability
	model=LogisticRegression()
	X=wp_df[['score_delta','home_team_flg','time_remaining']]
	y=[int(x) for x in wp_df.win_flg]
	model.fit(X,y) #Fit model
	print('Fit Model...')

	with open(fp+'/win_probability.sav', 'wb') as file:
    	pickle.dump(model, file)


def main():

	#Define filepath
	fp=os.path.dirname(os.path.realpath(__file__))
	
	#Set engine
	engine=get_engine(fp)

	#Get training data
	wp_df=get_data(engine,fp)

	#Build + save model
	build_save_model(wp_df,fp)


if __name__ == "__main__":
	main() 
