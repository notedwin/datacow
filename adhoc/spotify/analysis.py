import pandas as pd
import numpy as np
import matplotlib.cm as cm
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

import dotenv
import os
from sqlalchemy import create_engine
import logging

pd.set_option("display.max_columns", None)

dotenv.load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("spotify")
engine = create_engine(os.getenv(key="DATABASE_URL"))

def make_radar_chart(norm_df, n_clusters):
    fig = go.Figure()
    cmap = cm.get_cmap('tab20b')
    angles = list(norm_df.columns[5:])
    angles.append(angles[0])

    layoutdict = dict(
        radialaxis=dict(
            visible=True,
            range=[0, 1]
        )
    )
    maxes = dict()

    for i in range(n_clusters):
        subset = norm_df[norm_df['cluster'] == i]
        data = [np.mean(subset[col]) for col in angles[:-1]]
        maxes[i] = data.index(max(data))
        data.append(data[0])
        fig.add_trace(go.Scatterpolar(
            r=data,
            theta=angles,
            mode='lines',
            line_color='rgba' + str(cmap(i/n_clusters)),
            name="Cluster " + str(i)
        ))

    fig.update_layout(
        polar=layoutdict,
        showlegend=True
    )
    fig.update_traces()
    return fig, maxes


data = pd.read_sql_query(
    """
    SELECT track_name, album_name, release_date, 
    popularity, danceability, energy, key, loudness, mode, 
    speechiness, acousticness, instrumentalness, liveness, valence, tempo 
    FROM liked_songs l
    INNER JOIN audio_features a
    ON l.track_uri = a.uri
    LIMIT 100""",  
    engine,
)

# matplotlearn, sklearn
features = ['danceability', 'energy', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence']
norm_df = data[features].copy()
scaler = StandardScaler()
norm_df[features] = scaler.fit_transform(norm_df[features])

# Apply PCA
pca = PCA()
pca_features = pca.fit_transform(norm_df[features])
evr = pca.explained_variance_ratio_

plt.plot(range(1, len(norm_df.columns)+1), evr.cumsum(), marker='o', linestyle='--')
plt.xlabel('Number of Components', fontsize=18)
plt.ylabel('Cumulative Explained Variance',fontsize=18)
plt.xticks(fontsize=16)
plt.yticks(fontsize=16)
plt.show()



# Perform K-means clustering
n_clusters = 4  # Set the number of clusters you want
kmeans = KMeans(n_clusters=n_clusters, random_state=42)
norm_df['cluster'] = kmeans.fit_predict(norm_df[features])

# Add PCA components to the normalized dataframe
norm_df['PCA1'] = pca_features[:, 0]
norm_df['PCA2'] = pca_features[:, 1]

# Create the radar chart
fig, maxes = make_radar_chart(norm_df, n_clusters)

# Display the radar chart
fig.show()
