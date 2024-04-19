-- Which Channel has the highest amount of subscribers?
SELECT 
    ec.channel_name,
	ec.subscriber_count
FROM economy_channels ec
ORDER BY subscriber_count DESC
LIMIT 1;

--Which Video has the highest View count?
SELECT 
    ev.channel_name,
	ev.video_title,
	ev.viewcount AS highest_views
FROM economy_videos ev
ORDER BY viewcount DESC
LIMIT 1;

-- How many Videos has each channel published?
SELECT 
    channel_name, 
    video_count
FROM economy_channels;

-- Which is the Date of creation of the channel with most videos?
WITH max_video_channel AS(
    SELECT 	    
	    channel_name,
	    COUNT(video_title) as video_count
	FROM economy_videos
	GROUP BY channel_name
	ORDER BY video_count DESC
	LIMIT 1
)
SELECT
    ec.channel_name,
	ec.date_created
FROM economy_channels ec 
JOIN max_video_channel mvc ON ec.channel_name = mvc.channel_name;

-- Which Video has the highest amount of likes?
SELECT 
    channel_name,
	video_title,
	likecount AS highest_likes
FROM economy_videos
WHERE likecount = (SELECT MAX(likecount)
	               FROM economy_videos);

-- Which Channel has the highest amount of comments in total?
WITH max_channel_comment AS(   
    SELECT 
	    ev.channel_name,
		SUM(ev.commentcount) as channel_commentcount
	FROM economy_videos ev
	GROUP BY channel_name	
	)
	
SELECT 
    ec.channel_name,
    mcc.channel_commentcount
FROM economy_channels ec
JOIN max_channel_comment mcc ON ec.channel_name = mcc.channel_name
ORDER BY channel_commentcount DESC
LIMIT 1;

-- Which is most recent Video published in each channel?

--Subquery method:
SELECT 
    channel_name,
	video_title,
	date_published
FROM(
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY ev.channel_name ORDER BY ev.date_published DESC) AS row_number
    FROM economy_videos ev) AS newest_videos 
WHERE row_number = 1;

--CTE Method:
WITH newest_videos AS(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY ev.channel_name ORDER BY ev.date_published DESC) AS row_number
FROM economy_videos ev
)

SELECT channel_name, video_title,date_published
FROM newest_videos
 
WHERE row_number = 1;

-- Which are the Channels with most videos published in August? 
SELECT 
    channel_name,
    count(*) AS video_count_august
FROM economy_videos
WHERE EXTRACT(MONTH FROM date_published) = 8
GROUP BY channel_name;

-- Which are the videos with highest Likes to Views Ratio?
SELECT 
    ev.channel_name,
	ev.video_title,
	CASE
	    WHEN ev.viewcount <> 0 THEN ROUND((CAST(ev.likecount AS NUMERIC)/ev.viewcount * 100),2)
		ELSE NULL
	END AS like_view_ratio
FROM economy_videos ev
ORDER BY like_view_ratio DESC
LIMIT 10;

-- What is the Summary of the Views for each Channel?
SELECT 
    ec.channel_name,
	ROUND(AVG(ec.viewcount),2) AS mean_views,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ec.viewcount) AS median_views,
	ROUND(STDDEV(ec.viewcount),2) AS standard_deviation,
	MIN(ec.viewcount) AS least_viewed_video,
	MAX(ec.viewcount) AS most_viewed_video
FROM economy_videos ec 
GROUP BY ec.channel_name;
