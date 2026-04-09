
from sklearn.decomposition import PCA


class FeatureSelection:
    def __init__(self, n_components=25):
        self.pca = PCA(n_components=n_components)


    def fit(self, df):
        emb_cols = [col for col in df.columns if "_emb_" in col]
        self.pca.fit(df[emb_cols])
        self.emb_cols = emb_cols
        return self

    def transform(self, df):
        reduced = self.pca.transform(df[self.emb_cols])
        reduced_df = pd.DataFrame(reduced, columns=[f"pca_{i}" for i in range(reduced.shape[1])], index=df.index)
        df = df.drop(columns=self.emb_cols)
        df = pd.concat([df, reduced_df], axis=1)
        return df

    def fit_transform(self, df):
        return self.fit(df).transform(df)