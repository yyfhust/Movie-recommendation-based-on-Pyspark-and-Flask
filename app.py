
from flask import Flask, render_template
from flask import url_for
import os
import sys
from flask_sqlalchemy import SQLAlchemy  # 导入扩展类
import click
from flask import request
from flask import flash
from flask import redirect
from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
import random 
#import time, sys, cherrypy, os
from paste.translogger import TransLogger
#from app import create_app
#from pyspark import SparkContext, SparkConf
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


from recommendation_engine import RecommendationEngine
from pyspark import SparkConf, SparkContext, SQLContext

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dev'  # 等同于 app.secret_key = 'dev'

WIN = sys.platform.startswith('win')
if WIN:  # 如果是 Windows 系统，使用三个斜线
    prefix = 'sqlite:///'
else:  # 否则使用四个斜线
    prefix = 'sqlite:////'


app.config['SQLALCHEMY_DATABASE_URI'] = prefix + os.path.join(app.root_path, 'data.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # 关闭对模型修改的监控
# 在扩展类实例化前加载配置
db = SQLAlchemy(app)


##homepage (replace index!)
## show Top 10 most popular movies with poster!
@app.route('/', methods=['GET', 'POST'])
def home():
## To Do ###
### fetch Top 10 movies 
    # movies_top10 = recommender.get_top10() #
    sc = SparkContext.getOrCreate()
    global movie_recommender 
    global index_
    index_= 0
    movie_recommender = RecommendationEngine( sc,"datasets/ml-latest-small/"  )

    #movies_top10 = Movie_top10.query.all()  ## to be replaced by the above line
    movies_top10 = movie_recommender.top_10_popular_movies()

    db.drop_all()
    db.create_all()

    return render_template('home.html',movies_top=movies_top10)  ## parameters: movies is Top10 movies list


##page for recommendation
@app.route('/recommendation', methods=['GET', 'POST'])
def recommendation():
    # recommendation_list = recommender.get_top10recommendation()
    #recommendation_list =[]
    movie_recommender.train_engin()
    user_rated = Movie_rated.query.all()
    
    user_rating_list = list(map( lambda x: ( 0, x.movie_num,x.rating    )      ,user_rated))
    print(user_rating_list)
    print(">>>>>>>>>>>>>>>>>>")
    
    recommendation_list = movie_recommender.get_recommendation(user_rating_list)
    print (recommendation_list)
    return render_template('recommendation.html',movies = recommendation_list )



##page for rating
@app.route('/rating', methods=['GET', 'POST'])
def rating():
    ### at this moment, all(maybe partly) movies should be saved in the database
    #all_movies = Movie.query.all()
    all_movies = movie_recommender.get_all_movies()
    global movies_20
    global index_


    movies_20 = all_movies[ index_ * 10: (index_+1)*10] ## display 10 movies at one time
    index_ = index_+1 
    return render_template('rating.html',movies = movies_20 )



@app.route('/movie/edit/<int:movie_id>', methods=['GET', 'POST'])
def edit(movie_id):

    movie = movie_recommender.get_movie_by_ID(movie_id)[0]

    for i,item in enumerate(movies_20):
        if item[0] == movie_id:
            index = i
            
    if request.method == 'POST':  # 处理编辑表单的提交请求
        print("receive rating")
       # title = request.form['title']
        rating = request.form['rating']
        
      #  movie.rating = rating

        new_tuple= ( movie_id,movie[1],200, rating  )
        movies_20[index] = new_tuple
        #print(movies_20[index].rating)

        movie_rated = Movie_rated(title=movie[1], movie_num=movie[0],rating=rating)

        db.session.add(movie_rated)    
        db.session.commit()


        flash('rating finished')
        print ("redirect to index")
      #  return redirect(url_for('rating'))  # 重定向回主页
        return render_template('rating.html',movies = movies_20 )

    return render_template('edit.html', movie=movie)  # 传入被编辑的电影记录






class User(db.Model):  # 表名将会是 user（自动生成，小写处理）
    id = db.Column(db.Integer, primary_key=True)  # 主键
    name = db.Column(db.String(20))  # 名字

## movies rated by the user
class Movie_rated(db.Model):

    def __eq__(self, other) : 
        return self.__dict__ == other.__dict__
    id = db.Column(db.Integer, primary_key=True)  # 主键
    movie_num = db.Column(db.Integer)  ## id number of the movie
    title = db.Column(db.String(60))  # 电影标题
    rating = db.Column(db.Integer) ## rating of the movie by the user
 #   user_id = db.Column(db.Integer)  ## id of the user who rated this movie  




@app.context_processor
def inject_user():  # 函数名可以随意修改
    user = User.query.first()
    return dict(user=user)  # 需要返回字典，等同于return {'user': user}










