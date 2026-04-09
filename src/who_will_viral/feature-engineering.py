import pandas as pd

from who_will_viral.feature_engineering.feature_extraction import FeatureExtraction
from who_will_viral.feature_engineering.feature_scaling import FeatureScaling
from who_will_viral.feature_engineering.feature_selection import FeatureSelection



class FeatureEngineering:
    def __init__(self):
        self.cleaned_path = "/Users/ziadsamer/Documents/who-will-viral/data/youtube/cleaned_dataset.csv"
        self.df = pd.read_csv(self.cleaned_path, keep_default_na=False)

    def run(self):

        feature_extraction = FeatureExtraction(self.df)
        feature_extraction.run()
        print("hello")

FeatureEngineering().run()