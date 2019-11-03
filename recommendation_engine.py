import os
from pyspark.mllib.recommendation import ALS
import itertools
import math

class RecommendationEngine:
    """A movie recommendation engine"""

    def cal_Average_Rating(self):
    
        self.rating_group = self.ratings_RDD.map(lambda x: (x[1], x[2])).groupByKey()
        self.movie_average_rating_count  = self.rating_group.map(lambda x: (x[0],float(sum(w for w in x[1]))/len(x[1]), len(x[1])))


    def top_10_popular_movies(self,number=10):

        a = self.movie_average_rating_count.map(lambda x: (x[0], (x[1],x[2])  ))
        b = a.join(self.movies_RDD)
        c= b.map( lambda x: ( x[1][1], x[1][0][1] )  )
        d = c.takeOrdered(number,key = lambda x : -x[1])

        Top_list= list(map(  lambda x: ( dict(  title= x[0], rating_count = x[1] )   ), d  ))

        return Top_list

    def computeRmse(self,model, data, n):
        """
            Compute RMSE (Root Mean Squared Error).
        """
        predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
        predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])).join(data.map(lambda x: ((x[0], x[1]), x[2]))).values()
        return math.sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))


    def loadRating(self,path):
        ratings_raw_RDD = self.sc.textFile(path)
        ratings_raw_data_header = ratings_raw_RDD.take(1)[0]
        self.ratings_RDD = ratings_raw_RDD.filter(lambda line: line!=ratings_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),int(tokens[1]),float(tokens[2]))).cache()
        return self.ratings_RDD

    def loadMovies(self,path):
        movies_raw_RDD = self.sc.textFile(path)
        movies_raw_data_header = movies_raw_RDD.take(1)[0]
        self.movies_RDD = movies_raw_RDD.filter(lambda line: line!=movies_raw_data_header)\
            .map(lambda line: line.split(",")).map(lambda tokens: (int(tokens[0]),tokens[1],tokens[2])).cache()
        self.movies_titles_RDD = self.movies_RDD.map(lambda x: (int(x[0]),x[1])).cache()
        return self.movies_titles_RDD


    def __init__(self, sc, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        self.sc = sc

        # Load ratings data for later use

        ratings_file_path = os.path.join(dataset_path, 'ratings.csv')
        self.ratings_RDD = self.loadRating(ratings_file_path)

        # Load movies data for later use

        movies_file_path = os.path.join(dataset_path, 'movies.csv')
        self.movies_RDD = self.loadMovies(movies_file_path)

        # Pre-calculate movies ratings counts
        self.cal_Average_Rating()


    def train_engin(self,ifoptimized= False ):

        training_RDD, validation_RDD, test_RDD = self.ratings_RDD.randomSplit([6, 2, 2])
        validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
        test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

        if ifoptimized:
            rank_list = [ 2,3,4,5,6,7,8,9,10,11,12] 
            iterations_list = [5,10,15,20,25]
            lambda_list = [1,0.1,0.01,0.001]
            best_model = None
            low_error=1000
            for rank in rank_list:
                for iterrations in iterations_list:
                    for lamdba_ in lambda_list:
                        model = ALS.train(training_RDD, rank=rank, iterations=iterrations, lambda_= lamdba_ , blocks= -1, nonnegative=False, seed=None)
                        predictions = self.model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
                        rates_and_preds = validation_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
                        error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())  
                        if error < low_error:
                            best_model = model
                            low_error = error
                            self.rank = rank
                            self.iterations = iterrations
                            self.regularization_parameter= lamdba_

            self.model= best_model

        else:

            self.rank = 8
            #seed = 10
            self.iterations = 10
            self.regularization_parameter = 0.1
            self.model = ALS.train(training_RDD, rank = self.rank, iterations= self.iterations, lambda_= self.regularization_parameter)

        ## evaluation the model
        predictions = self.model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
        rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
        error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    
        print('For testing data the RMSE is %s' % (error))



    def get_recommendation(self, user_ratings ,number=10,min_rating_num = 100):
        ## generate recommendation: default number is 10
        user_ratings_RDD = self.sc.parallelize(user_ratings)
        self.ratings_RDD = self.ratings_RDD.union(user_ratings_RDD)

        self.model = ALS.train(self.ratings_RDD, rank = self.rank, iterations= self.iterations, lambda_= self.regularization_parameter)

        movie_id_rated = list(map(lambda x: x[1], user_ratings)) 
        unrated_movies = (self.ratings_RDD.filter(lambda x: x[0] not in movie_id_rated).map(lambda x: (0, x[1]))).distinct()
        recommendations= self.model.predictAll(unrated_movies)
        recommendations = recommendations.map(lambda x: (x.product, x.rating))
        recommendations_with_count = recommendations.join(self.movie_average_rating_count.map(lambda x: (x[0],x[2])))
        temp1 = recommendations_with_count.filter(lambda x: x[1][1]>=min_rating_num)
        temp2 = temp1.join(self.movies_titles_RDD).map( lambda x: (x[0], x[1][1],x[1][0][0] ) )
        temp3 = temp2.takeOrdered(number, key=lambda x: -x[2])
        return temp3
    

    def retrain_engin(self):
        ## retrain the model on all the dataset
        self.model = ALS.train(self.ratings_RDD, rank = self.rank, iterations= self.iterations, lambda_= self.regularization_parameter)

    def get_all_movies( self ,min_rating_num=100):
        all_movies = self.movies_titles_RDD.join( self.movie_average_rating_count.map( lambda x: (x[0],x[2]) ) ).map(  lambda x: (x[0],x[1][0],x[1][1],"Not rated by you"))
        movies = all_movies.filter( lambda x: x[2]>min_rating_num )
        return movies.takeOrdered(2000, key = lambda x: -x[2])


    def get_movie_by_ID(self, movie_id):

        return self.movies_RDD.filter( lambda x: x[0 ]== movie_id  ).collect()
    








