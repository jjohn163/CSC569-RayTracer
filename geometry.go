package main

import "math"

type Sphere struct{
	position 	vec3
	radius		float64
	color		vec3
	reflectance float64
}

func (s Sphere) checkHit(ray *Ray, hit *Hit) bool {
	dir := ray.position.add(s.position.multiply(-1.0))
	a := ray.direction.dot(ray.direction)
	b := 2.0 * ray.direction.dot(dir)
	c := dir.dot(dir) - s.radius * s.radius
	discriminant := b * b - 4.0 * a * c
	if discriminant >= 0 { //ray hit sphere
		t := (-b - math.Sqrt(discriminant)) / (2.0 * a)
		if t < hit.t && t > 0.01 { //hit was closer than any previous hit
			hit.t = t
			newRayPosition := ray.position.add(ray.direction.multiply(t))
			newRayDirection := newRayPosition.add(s.position.multiply(-1.0))
			newRayDirection = newRayDirection.normalize()
			hit.ray = Ray{newRayPosition, newRayDirection}
			hit.color = s.color
			hit.reflectance = s.reflectance
			return true
		}
	}
	return false
}

type Plane struct{
	normal 		vec3
	height		float64
	color		vec3
	reflectance float64
}

func (p Plane) checkHit(ray *Ray, hit *Hit) bool {
	b := ray.direction.dot(p.normal)
	if b != 0 { //ray hit plane
		t := (p.height - ray.position.dot(p.normal)) / b
		if t < hit.t && t > 0.01 { //hit was closer than any previous hit
			hit.t = t
			newRayPosition := ray.position.add(ray.direction.multiply(t))
			newRayDirection := p.normal
			newRayDirection = newRayDirection.normalize()
			hit.ray = Ray{newRayPosition, newRayDirection}
			hit.color = p.color
			hit.reflectance = p.reflectance
			return true
		}
	}
	return false
}
