import pandas as pd

def transform(input_path: str = "tracks_raw.csv",
              output_path: str = "tracks_transformed.csv") -> pd.DataFrame:
    """
    Transform raw Spotify track data into analytics-ready format.
    Produces exactly the 10 columns that Snowflake expects.
    """

    # 1. Load raw data
    df = pd.read_csv(input_path)

    # 2. Cleaning
    df = df.dropna(subset=["track_id"])            # remove records without ID
    df = df.drop_duplicates(subset=["track_id"])   # avoid duplicates

    # 3. Standardize column names (if needed)
    df = df.rename(columns={
        "album": "album_name",
        "track": "track_name"
    })

    # 4a. Duration in minutes
    df["duration_minutes"] = (df["duration_ms"] / 1000 / 60).round(2)

    # 4b. Length category
    def categorize_length(m):
        if m < 3:
            return "Short (<3 min)"
        elif 3 <= m <= 5:
            return "Medium (3-5 min)"
        else:
            return "Long (>5 min)"

    df["length_category"] = df["duration_minutes"].apply(categorize_length)

    # 4c. Album-level track count
    album_track_counts = df.groupby("album_name")["track_id"].nunique()
    df["album_track_count"] = df["album_name"].map(album_track_counts)

    # 4d. Album popularity rank
    album_rank = (
        album_track_counts
        .sort_values(ascending=False)
        .rank(method="dense", ascending=False)
    )
    df["album_popularity_rank"] = df["album_name"].map(album_rank).astype(int)

    # 🎯 VERY IMPORTANT — final schema for Snowflake (10 columns)
    final_columns = [
        "artist",
        "album_name",
        "track_name",
        "track_id",
        "duration_ms",
        "explicit",
        "duration_minutes",
        "length_category",
        "album_track_count",
        "album_popularity_rank"
    ]
    df = df[final_columns]

    # 5. Save
    df.to_csv(output_path, index=False)

    return df


if __name__ == "__main__":
    transformed_df = transform()
    print("\nTransformed data preview:")
    print(transformed_df.head())
    print("\nSaved to 'tracks_transformed.csv'")