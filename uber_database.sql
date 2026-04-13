create database uber;
use uber;

select * from cancellations;
select * from drivers;
select * from locations;
select * from payments;
select * from reviews;
select * from riders;
select * from trips;
select * from users;

/* overew metrics */ 

/* total number of trips, total revenue, and average fare across all cities */

with trips_data as (select ts.trip_id, ts.total_fare, ls.city 
					from trips ts
                    join locations ls on ts.pickup_location_id = ls.location_id
                    where ts.status = 'completed' )

select count(trip_id) as total_trips, sum(total_fare) as total_revenue, avg(total_fare) as avg_fare
from trips_data;

/* percentage of users who are drivers vs riders */

with users_drivers_riders as (select count(is_driver) as total_users,
							sum(case when is_driver = 1 then 1 else 0 end) as total_driver
                            from users)
                            
                            
select total_users, total_driver, (total_users - total_driver) as total_riders,  round((total_driver * 100 / total_users),2) as pct_driver, round(((total_users - total_driver ) * 100 / total_users),2) as pct_user  
from users_drivers_riders;

/* average rating of drivers compared to average rating of riders */

with driver_avg  as (select avg(rating) as avg_rating_drivers
					from drivers
), 
riders_avg as (select avg(rating) as avg_rating_riders
				from riders
)

select da.avg_rating_drivers, ra.avg_rating_riders
from driver_avg da 
cross join riders_avg ra;

/* total number of trips and total revenue per city */

select ls.city as cities, count(trip_id) as total_trips, round(sum(total_fare),2) as total_revenue
from trips ts
left join locations ls on ts.pickup_location_id = ls.location_id
group by cities;

/* contribution of each payment method to total revenue (percentage share) */

with total as (select method, sum(amount) as total_revenue 
						from payments
                        group by method
)

select method, round((total_revenue),2), ROUND(total_revenue * 100.0 / SUM(total_revenue) OVER (), 2) AS pct_revenue
from total; 

/* Operational Insights */ 

/* average trip duration and distance by city, compared to overall averages */

with average as (select ls.city as cities, avg(ts.duration_mins) as avg_duration_min, avg(ts.distance_km) as avg_distance_km
				from trips ts
                join locations ls on ts.pickup_location_id = ls.location_id
                group by cities
)

select cities, round(avg_duration_min, 2) as avg_duration_min, round(avg_distance_km,2) as avg_distance_km, 
		round(avg(avg_duration_min) over(), 2) as avg_duration_overall, round(avg(avg_distance_km) over(), 2) as avg_distance_overall 
from average;
		
/* trips where duration is above the average duration of all trips */

select trip_id, duration_mins
from trips 
where duration_mins > (select avg(duration_mins)
						from trips);
                                               
/* percentage of cancelled trips by cancellation reason */

select reason, count(*) as total_cancelled, round(count(*) * 100 / sum(count(*)) over(), 2) as pct_cancelled 
from cancellations
group by reason;

/* drivers with trip counts above the average number of trips per driver */

with count_trips as (select driver_id, count(trip_id) as total_trips
					from trips
                    group by driver_id) 
 
 select driver_id, total_trips
 from count_trips
 where total_trips > (select avg(total_trips)
						from count_trips);

/* identify trips where the actual duration is significantly higher than average for that distance */

select trip_id, duration_mins, distance_km
from trips 
where duration_mins > (select avg(distance_km)
						from trips);

/* payment methods with success rate below overall average payment success rate */
                        
select * 
from (select method, sum(status = 'success') / count(*) as success_rate, avg(sum(status = 'success') / count(*)) over() as avg_success_rate 
		from payments
        group by method 
) t
where success_rate < avg_success_rate;

/* Strategic Insights */
/* top 5 drivers by total revenue generated compared to average driver revenue */

with driver_revenue as (select driver_id, SUM(total_fare) as total_revenue
						from trips
						where status = 'completed'
						group by driver_id
)

select driver_id,total_revenue, round(avg(total_revenue) over (), 2) as avg_driver_revenue, round(total_revenue - avg(total_revenue) over (), 2) as diff_from_avg
from driver_revenue
order by total_revenue desc
limit 5;

/* cities where average fare is higher than the global average fare */

select * from (select ls.city, round(avg(ts.total_fare), 2) as avg_city_fare, ROUND(avg(avg(ts.total_fare)) over (), 2) as global_avg_fare
				from trips ts
				join locations ls on ts.pickup_location_id = ls.location_id
				where ts.status = 'completed'
				group by ls.city
) t
where avg_city_fare > global_avg_fare;

/* peak hours ranked by total number of trips using window functions */

select hour,total_trips, rank() over (order by total_trips desc) as rank_hour
from (select hour(requested_at) as hour, count(*) as total_trips
		from trips
		group by hour(requested_at)
) t;

/* zones with highest pickup demand and their percentage contribution to total trips */

select zone_name, total_trips, round(total_trips * 100.0 / sum(total_trips) over (), 2) as pct_contribution
from (select l.zone_name, COUNT(*) as total_trips
    from trips t
    join locations l on t.pickup_location_id = l.location_id
    group by l.zone_name
) t
order by total_trips desc;

/* impact of surge pricing: compare average fare for trips with surge > 1 vs no surge */

select case when surge_multiplier > 1 then 'Surge' else 'No Surge' end as surge_type, round(avg(total_fare), 2) as avg_fare
from trips
where status = 'completed'
group by surge_type;

/* drivers whose ratings are above the average rating within their city */

select *
from (select d.driver_id, u.city, d.rating, avg(d.rating) over (partition by u.city) as avg_city_rating
        from drivers d join users u on d.user_id = u.user_id
) t
where rating > avg_city_rating;

/* trips that generated above-average revenue within their respective city */

select *
from (select ts.trip_id, ls.city, ts.total_fare, avg(ts.total_fare) over (partition by ls.city) as avg_city_fare
		from trips ts
		join locations ls on ts.pickup_location_id = ls.location_id
		where ts.status = 'completed'
) t
where total_fare > avg_city_fare;

/* Predictive / Trend Analysis */
/* monthly growth in number of trips using a window function (month-over-month change) */

with montlhy_trips as (select date_format(requested_at, '%Y-%m') as months,
						count(*) as total_trip 
                        from trips 
                        group by date_format(requested_at, '%Y-%m')
)

select months, total_trip, 
		lag(total_trip) over(order by months) as prev_months_trips, 
        total_trip - lag(total_trip) over(order by months) as trip_changes, 
        round((total_trip - lag(total_trip) over(order by months)) * 100 / lag(total_trip) over(order by months), 2 ) as pct_growth 
from montlhy_trips;

/* trend of total revenue over time and percentage increase/decrease between periods */

select month, total_revenue, lag(total_revenue) over (order by month) as prev_revenue,
        total_revenue - lag(total_revenue) over (order by month) AS revenue_change,
        ROUND((total_revenue - lag(total_revenue) over (order by month)) * 100.0 / lag(total_revenue) over (order by month), 2) as pct_change

from (select date_format(requested_at, '%Y-%m') as month, SUM(total_fare) as total_revenue
		from trips
		where status = 'completed'
		group by date_format(requested_at, '%Y-%m')
) t;

/* identify days where trip volume is significantly higher than the moving average */

with daily_trips as (select date(requested_at) as trip_date, COUNT(*) as total_trips
					from trips
					group by date(requested_at)
)

select *
from (select trip_date, total_trips, 
		avg(total_trips) over (order by trip_date rows between 6 preceding and current row) as moving_avg
        
    from daily_trips
) t
where total_trips > moving_avg ;

/* drivers whose activity (trip count) is declining over time */

with driver_monthly_trips as (select driver_id, date_format(requested_at, '%Y-%m') as month, COUNT(*) as total_trips
							from trips
							group by driver_id, date_format(requested_at, '%Y-%m')
),

with_lag as (select driver_id, month, total_trips, 
			lag(total_trips) over (partition by driver_id order by month) as prev_trips
			from driver_monthly_trips
)

select driver_id, month, total_trips, prev_trips
from with_lag
where total_trips < prev_trips;


/* Segmentation / Customer Value Analysis */

/* segment riders into high, medium, and low frequency based on total trips using case statements */
with  riders_trips as (select rider_id, count(*) as total_riders_trips 
						from trips 
                        group by rider_id
) 

select rider_id, total_riders_trips, 
		case when total_riders_trips >= 70 then 'High'
        when total_riders_trips between 35 and 69 then 'Medium'
        else 'Low'
		end as  rider_segment
from riders_trips;

/* top 10% riders based on total spending using window functions (NTILE or PERCENT_RANK) */

with rider_spending as (select r.rider_id, SUM(t.total_fare) as total_spent
						from trips t
						join riders r on t.rider_id = r.rider_id
						where t.status = 'completed'
						group by  r.rider_id
)

select * 
from (select rider_id, total_spent, ntile(10) over (order by total_spent  desc) as percentile_group
    from rider_spending
) t
where percentile_group = 1;

/* compare average rating between high-frequency riders and low-frequency riders */

with rider_segments as (select rider_id, total_trips, rating,
						case when total_trips >= 70 then 'High'
						when total_trips < 35 then 'Low'
						else 'Medium'
						end as segment
						from riders
)

select segment, round(avg(rating), 2) as avg_rating
from rider_segments
where segment in ('High', 'Low')
group by segment;

/* drivers grouped into performance tiers based on rating and total trips */

with driver_stats as (select d.driver_id, d.rating, COUNT(t.trip_id) as total_trips
					from drivers d
					left join trips t on d.driver_id = t.driver_id
					group by d.driver_id, d.rating
)

select driver_id, rating, total_trips,
		case when rating >= 4.5 and total_trips >= 50 then 'Top Performer'
        when rating >= 4.0 and total_trips >= 20 then 'Good Performer'
        else 'Low Performer'
		end as performance_tier
from driver_stats;
