
--group by
select behavior,count(*) from user_behavior group by behavior;
--tumble window
INSERT INTO buy_cnt_per_hour
SELECT HOUR(TUMBLE_START(ts, INTERVAL '1' HOUR)), COUNT(*)
FROM user_behavior
WHERE behavior = 'buy'
GROUP BY TUMBLE(ts, INTERVAL '1' HOUR);
--over window
CREATE VIEW uv_per_10min AS
SELECT
  MAX(SUBSTR(DATE_FORMAT(ts, 'HH:mm'),1,4) || '0') OVER w AS time_str,
  COUNT(DISTINCT user_id) OVER w AS uv
FROM user_behavior
WINDOW w AS (ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

INSERT INTO cumulative_uv
SELECT time_str, MAX(uv)
FROM uv_per_10min
GROUP BY time_str;
--temproal table join
CREATE VIEW rich_user_behavior AS
SELECT U.user_id, U.item_id, U.behavior,
  CASE C.parent_category_id
    WHEN 1 THEN '服饰鞋包'
    WHEN 2 THEN '家装家饰'
    WHEN 3 THEN '家电'
    WHEN 4 THEN '美妆'
    WHEN 5 THEN '母婴'
    WHEN 6 THEN '3C数码'
    WHEN 7 THEN '运动户外'
    WHEN 8 THEN '食品'
    ELSE '其他'
  END AS category_name
FROM user_behavior AS U LEFT JOIN category_dim FOR SYSTEM_TIME AS OF U.proctime AS C
ON U.category_id = C.sub_category_id;

INSERT INTO top_category
SELECT category_name, COUNT(*) buy_cnt
FROM rich_user_behavior
WHERE behavior = 'buy'
GROUP BY category_name;