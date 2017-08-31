/*
Problem statement
  1. Get rank of each category by revenue with in each department generated from all the transactions
        Display the results by deparment_name and rank in ascending order
  2. Get percentage of each category by revenue with in each department
        Display the results by department_name and percentage in descending order
*/


hiveContext.sql("select d_name, c_name, revenue, rank() over (partition by d_name order by revenue desc) as rank_ from ( select d.department_name as d_name, c.category_name as c_name, sum(oi.order_item_subtotal) as revenue from departments d join categories c on ( c.category_department_id = d.department_id) join products p on ( p.product_category_id = c.category_id) join order_items oi on ( oi.order_item_product_id = p.product_id) group by d.department_name, c.category_name order by d.department_name ) q1")

/*
+--------+--------------------+------------------+-----+
|  d_name|              c_name|           revenue|rank_|
+--------+--------------------+------------------+-----+
| Apparel|              Cleats| 4431942.660001187|    1|
| Apparel|      Men's Footwear| 2891757.540001228|    2|
|Fan Shop|             Fishing| 6929653.500002877|    1|
|Fan Shop|    Camping & Hiking|4118425.4199997648|    2|
|Fan Shop|        Water Sports|3113844.6000010464|    3|
|Fan Shop|Indoor/Outdoor Games| 2888993.939999287|    4|
|Fan Shop|  Hunting & Shooting| 56848.41999999991|    5|
| Fitness| Baseball & Softball| 94057.15000000017|    1|
| Fitness|              Hockey|48360.729999999996|    2|
| Fitness|    Tennis & Racquet|44585.090000000026|    3|
| Fitness|            Lacrosse|39464.790000000045|    4|
| Fitness|          Basketball| 27099.33000000004|    5|
| Fitness|              Soccer|26477.049999999992|    6|
|Footwear|    Cardio Equipment|3694843.2000004393|    1|
|Footwear|         Electronics|115355.25000000057|    2|
|Footwear|        Boxing & MMA| 85205.41000000003|    3|
|Footwear|   Strength Training|          54895.53|    4|
|Footwear| Fitness Accessories| 35601.44000000006|    5|
|Footwear|     As Seen on  TV!|20597.939999999995|    6|
|    Golf|     Women's Apparel|         3147800.0|    1|
+--------+--------------------+------------------+-----+
*/

hiveContext.sql("select d_name, c_name, revenue, revenue/(sum(revenue) over (partition by d_name rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING)) as pct_ from ( select d.department_name as d_name, c.category_name as c_name, sum(oi.order_item_subtotal) as revenue from departments d join categories c on ( c.category_department_id = d.department_id) join products p on ( p.product_category_id = c.category_id) join order_items oi on ( oi.order_item_product_id = p.product_id) group by d.department_name, c.category_name order by d.department_name ) q1")

/*
+--------+--------------------+------------------+--------------------+
|  d_name|              c_name|           revenue|                pct_|
+--------+--------------------+------------------+--------------------+
| Apparel|              Cleats| 4431942.660001187|  0.6051507487976808|
| Apparel|      Men's Footwear| 2891757.540001228| 0.39484925120231906|
|Fan Shop|             Fishing| 6929653.500002877|   0.405058939233138|
|Fan Shop|  Hunting & Shooting| 56848.41999999991|0.003322959900126...|
|Fan Shop|        Water Sports|3113844.6000010464| 0.18201351490557718|
|Fan Shop|    Camping & Hiking|4118425.4199997648|  0.2407342635436538|
|Fan Shop|Indoor/Outdoor Games| 2888993.939999287|  0.1688703224175046|
| Fitness|    Tennis & Racquet|44585.090000000026| 0.15920736638159966|
| Fitness|              Soccer|26477.049999999992| 0.09454598835740669|
| Fitness|          Basketball| 27099.33000000004|  0.0967680666340671|
| Fitness|              Hockey|48360.729999999996| 0.17268966956423354|
| Fitness|            Lacrosse|39464.790000000045| 0.14092346299408376|
| Fitness| Baseball & Softball| 94057.15000000017| 0.33586544606860935|
|Footwear| Fitness Accessories| 35601.44000000006|0.008885923107370915|
|Footwear|         Electronics|115355.25000000057|0.028792034297813573|
|Footwear|     As Seen on  TV!|20597.939999999995|0.005141132241005962|
|Footwear|   Strength Training|          54895.53| 0.01370162157818258|
|Footwear|        Boxing & MMA| 85205.41000000003| 0.02126680048874461|
|Footwear|    Cardio Equipment|3694843.2000004393|  0.9222124882868823|
|    Golf|       Shop By Sport|1309522.0199998142|  0.2841210679330288|
|    Golf|     Women's Apparel|         3147800.0|  0.6829639242261196|
|    Golf|      Girls' Apparel|151706.20000000024|0.032915007840851614|
|Outdoors|          Golf Balls| 77098.16000000025| 0.07744023520215372|
|Outdoors|            Trade-In| 68721.77999999996| 0.06902669021816658|
|Outdoors|         Electronics|255679.39000000144| 0.25681380850001134|
|Outdoors|    Kids' Golf Clubs| 98797.58000000042|   0.099235932901688|
|Outdoors|        Golf Apparel|34969.750000000124| 0.03512490654719277|
|Outdoors|          Golf Shoes|          107998.0| 0.10847717405139332|
|Outdoors|    Men's Golf Clubs|47035.799999999894| 0.04724449215028532|
|Outdoors|         Accessories|133671.51000000126| 0.13426459430714177|
|Outdoors|  Women's Golf Clubs| 44545.96999999997| 0.04474361507600272|
|Outdoors|   Golf Bags & Carts| 10369.38999999999|0.010415397728076226|
|Outdoors|         Golf Gloves|116695.39000000048| 0.11721315331788806|
+--------+--------------------+------------------+--------------------+

*/
