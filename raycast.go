package main

import "math/rand"
import "log"

type Ray struct{
	position 	vec3
	direction	vec3
}

type Hit struct{
	ray			Ray
	t			float64
	color		vec3
	reflectance float64
}

func randomDirection() vec3 {
	for true {
		direction := vec3{rand.Float64() * 2.0 - 1.0, rand.Float64() * 2.0 - 1.0, rand.Float64() * 2.0 - 1.0}
		if direction.length() <= 1 {
			return direction
		}
	}
	log.Fatal("couldn't generate random direction")
	return vec3{0,0,0}
}

func reflect(ray *Ray, hit *Hit) {
	dir := ray.direction.add(hit.ray.direction.multiply(-2.0 * hit.ray.direction.dot(ray.direction)))
	dir = dir.normalize()
	dir = hit.ray.position.add(dir).add(randomDirection().multiply(hit.reflectance))
	dir = dir.add(hit.ray.position.multiply(-1.0))
	ray.direction = dir.normalize()
	ray.position = hit.ray.position
}

