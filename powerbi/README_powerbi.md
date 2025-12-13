🎵 Spotify Artist Popularity & Track Insights – Power BI Dashboard

A 5-page analytical dashboard exploring artist output, song characteristics, and popularity signals using curated Spotify tracks data.

📌 Project Overview

This Power BI project analyzes 1,000 Spotify tracks across multiple popular artists, uncovering patterns in song duration, explicit content, album output, artist performance, and overall catalog trends.
The dashboard is designed using industry-grade BI practices, including DAX measures, drill-throughs, slicers, and page-level insights.

This project demonstrates end-to-end data modeling, DAX, visual design, and storytelling — aligned with skills required for BI Analyst, Data Analyst, and Analytics Engineer roles.

🛠 Tools & Technologies
	•	Power BI Desktop
	•	Power Query
	•	DAX (Data Analysis Expressions)
	•	Advanced Visual Analytics
	•	Data Modeling
	•	CSV Dataset (Spotify Tracks – Curated)


    📊 Dashboard Pages

The report contains 5 fully designed pages, each solving a specific business question.

1️⃣ Artist Overview Dashboard

“Comparing key artists by number of tracks, song length, and popularity.”

Key visuals:
	•	Total tracks by artist
	•	Average song duration by artist
	•	Average popularity (0–100)
	•	Popularity vs duration scatter chart

Questions answered:
	•	Which artists produce the most songs?
	•	Who has the longest average tracks?
	•	Who is the most popular?
	•	How do duration and popularity correlate?


    2️⃣ Track Analytics Dashboard

“How albums and tracks vary by output, length distribution, and explicit content.”

Key visuals:
	•	Song length categories (Short / Medium / Long)
	•	Explicit vs Clean songs distribution
	•	Track counts by album
	•	Track counts by artist
	•	Total duration contributed by each artist

Questions answered:
	•	How long are most Spotify songs?
	•	Which albums contain the highest number of tracks?
	•	How many songs are explicit?
	•	Which artists contribute the largest playtime?


    3️⃣ Song Deep Analytics Dashboard

“Detailed analysis of song duration patterns and explicit content behavior.”

Key visuals:
	•	Top 10 longest songs
	•	Explicit vs Clean average duration comparison
	•	Song length category donut
	•	Average duration by artist

Questions answered:
	•	Are explicit songs typically longer or shorter?
	•	Which songs are the longest overall?
	•	Which artists produce longer tracks?


    4️⃣ Artist Deep Dive Dashboard

“Drill-down view for each artist individually.”

Features:
	•	Artist slicer to filter entire page
	•	Song duration distribution histogram
	•	Album-wise track counts
	•	Top 10 longest tracks for the selected artist

Questions answered:
	•	What does each artist’s catalog look like?
	•	Which albums are most productive?
	•	How do their song durations vary?


    5️⃣ Global Summary Dashboard

“Executive-level combined insights across all Spotify tracks.”

Includes:
	•	Total songs
	•	Most popular artist
	•	Album with most songs
	•	Explicit vs Clean distribution
	•	Global song duration distribution
	•	Artist average duration heatmap
	•	Key insights summary

A fully polished, presentation-ready summary page.


🧮 Key DAX Measures Used

Total Songs = COUNT(spotify_tracks[track_name])

Avg Song Duration = AVERAGE(spotify_tracks[duration_minutes])

Explicit Percentage = 
DIVIDE(
    CALCULATE(COUNTROWS(spotify_tracks), spotify_tracks[explicit] = TRUE()),
    COUNTROWS(spotify_tracks)
) * 100

Top Artist Name = 
CALCULATE(
    FIRSTNONBLANK(spotify_popularity_summary[artist], 1),
    FILTER(
        spotify_popularity_summary,
        spotify_popularity_summary[avg_popularity] = MAX(spotify_popularity_summary[avg_popularity])
    )
)


🧠 Key Insights from the Analysis
	•	Most songs fall between 3–5 minutes, with very few extremely short or long tracks.
	•	Song durations cluster around 3–4 minutes, visible in the histogram.
	•	Clean songs are the majority, but explicit tracks still represent ~40% of the dataset.
	•	Explicit songs tend to run slightly longer on average.
	•	A limited number of albums contribute the highest output, creating uneven song distribution.
	•	The Weeknd, Drake, Taylor Swift, and Ed Sheeran contribute significant catalog depth.