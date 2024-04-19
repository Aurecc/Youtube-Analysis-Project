CREATE VIEW economy_channels AS(
    SELECT *
    FROM channels c
    WHERE id_channel IN ('UCCVIYA5kpLvEToE8Gj8Fszw','UCvSXMi2LebwJEM1s4bz5IBA','UCZ4AMrDcNrfy3X6nsU8-rPg','UCkCGANrihzExmu9QiqZpPlQ','UCBLCvUUCiSqBCEc-TqZ9rGw')
	);

CREATE VIEW economy_videos AS(
    SELECT v.* 
    FROM videos v 
    JOIN economy_channels ec ON v.channel_name = ec.channel_name
    ); 
