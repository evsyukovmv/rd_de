-- 1. вывести количество фильмов в каждой категории, отсортировать по убыванию.
select c.category_id, c.name, count(fc.film_id) as films_count from category as c
join film_category fc on c.category_id = fc.category_id
group by c.category_id, c.name
order by films_count desc;


-- 2. вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
select a.actor_id, a.first_name, a.last_name, sum(f.rental_duration) as films_rental_duration from actor as a
join film_actor fa on a.actor_id = fa.actor_id
join film f on fa.film_id = f.film_id
group by a.actor_id, a.first_name, a.last_name
order by films_rental_duration desc
limit 10;


-- 3. вывести категорию фильмов, на которую потратили больше всего денег.
select c.name, sum(p.amount) as sum_payment from payment AS p
join rental r on p.rental_id = r.rental_id
join inventory i on r.inventory_id = i.inventory_id
join film_category fc on i.film_id = fc.film_id
join category c on fc.category_id = c.category_id
group by c.name
order by sum_payment desc
limit 1;

-- 4. вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.
select f.film_id, f.title from film AS f
left join inventory i on f.film_id = i.film_id
where inventory_id is null;


-- 5. вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
-- Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.
select res.*
from (
select a.first_name, a.last_name, count(fa.film_id) as films_count, DENSE_RANK() OVER (ORDER BY count(fa.film_id) desc)
              AS rank from actor as a
join film_actor fa on a.actor_id = fa.actor_id
join film_category fc on fa.film_id = fc.film_id
join category c on fc.category_id = c.category_id
where c.name = 'Children'
group by a.first_name, a.last_name
order by rank
) as res
where res.rank <= 3;


-- 6. вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
-- Отсортировать по количеству неактивных клиентов по убыванию.
select c.city,
       sum(case cus.active when 1 then 1 else 0 end) as active_count,
       sum(case cus.active when 0 then 1 else 0 end) as non_active_count
from city as c
join address a on c.city_id = a.city_id
join customer cus on a.address_id = cus.address_id
group by c.city
order by non_active_count desc;


-- 7. вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах
-- (customer.address_id в этом city), и которые начинаются на букву “a”.
-- То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.
(select 'a%' as city_filter, c.name category_name, sum(r.return_date - r.rental_date) rent_duration
from category as c
       join film_category fc on c.category_id = fc.category_id
       join inventory i on fc.film_id = i.film_id
       join rental r on i.inventory_id = r.inventory_id
       join customer on r.customer_id = customer.customer_id
       join address a on customer.address_id = a.address_id
       join city on a.city_id = city.city_id
where city.city ilike 'a%'
group by c.name
order by rent_duration desc
limit 1)
union
(select '%-%' as city_filter, c.name category_name, sum(r.return_date - r.rental_date) rent_duration
from category as c
       join film_category fc on c.category_id = fc.category_id
       join inventory i on fc.film_id = i.film_id
       join rental r on i.inventory_id = r.inventory_id
       join customer on r.customer_id = customer.customer_id
       join address a on customer.address_id = a.address_id
       join city on a.city_id = city.city_id
where city.city ilike '%-%'
group by c.name
order by rent_duration desc
limit 1)


