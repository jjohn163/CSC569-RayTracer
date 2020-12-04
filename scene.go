package main

import "math/rand"
import "math"

type Camera struct{
	position	vec3
	horizontal	vec3
	vertical	vec3
	llc			vec3
}
func createCamera(pos vec3, lookAt vec3, up vec3, right vec3) Camera {
	w := pos.add(lookAt.multiply(-1.0))
	w = w.normalize()
	u := up.cross(w)
	v := w.cross(u)
	horizontal := u.multiply(right.length())
	vertical := v.multiply(up.length())
	llc := pos.add(horizontal.multiply(-0.5)).add(vertical.multiply(-0.5)).add(w.multiply(-1.0))
	return Camera{pos, horizontal, vertical, llc}
}

type Scene struct{
	spheres	[]Sphere
	planes	[]Plane
	camera	Camera
	resX	int
	resY	int
}

func scene1() Scene{
	spheres := []Sphere{Sphere{vec3{-1,0,2}, 1, vec3{1,0,0}, 1.0}, Sphere{vec3{3,1,0}, 3, vec3{1,0.5,0}, 0.1}, Sphere{vec3{-6,2,-5}, 4, vec3{1.0,1.0,1.0}, 0.0}}
	planes := []Plane{Plane{vec3{0,1,0}, -2, vec3{0.0,0.5,0.2}, 1.0}}
	camera := createCamera(vec3{0,0,10}, vec3{0,0,0}, vec3{0,1,0}, vec3{1.33333,0,0})
	return Scene{spheres, planes, camera, 640, 480}
}

func rayColor(ray *Ray) vec3 { //gives gradient to the background
	r := ray.direction.normalize()
	t := 0.5*(r.y + 1.0);
	a := vec3{1.0, 1.0, 1.0}
	b := vec3{0.4, 0.4, 1.0}
    return a.multiply(1.0-t).add(b.multiply(t))
}

func (scene Scene) shadeRay(ray *Ray, depth int) vec3 {
	if depth == 0 {
		return vec3{0,0,0}
	}
	hit := Hit{Ray{vec3{0,0,0}, vec3{0,0,0}}, 1000000.0, rayColor(ray), 1.0} //default hit results
	collision := false
	for _, sphere := range scene.spheres {
		collision = collision || sphere.checkHit(ray, &hit)
	}
	for _, plane := range scene.planes {
		collision = collision || plane.checkHit(ray, &hit)
	}
	if collision {
		reflect(ray, &hit)
		return scene.shadeRay(ray, depth - 1).multiplyVec3(hit.color)
	} else {
		return hit.color
	}
}

func (scene Scene) trace(x int, y int, samples int, depth int) vec3 {
	color := vec3{0,0,0}
	for sample := 0; sample < samples; sample++ {
		vp := (float64(y) + (rand.Float64() * 2.0 - 1.0)) / float64(scene.resY);
		hp := (float64(x) + (rand.Float64() * 2.0 - 1.0)) / float64(scene.resX);
		rayDirection := scene.camera.llc.add(scene.camera.horizontal.multiply(hp)).add(scene.camera.vertical.multiply(vp)).add(scene.camera.position.multiply(-1.0))
		ray := Ray{scene.camera.position, rayDirection}
		color = color.add(scene.shadeRay(&ray, depth))
	}
	color = color.divide(float64(samples))
	color.x = math.Min(math.Sqrt(color.x), 1.0)
	color.y = math.Min(math.Sqrt(color.y), 1.0)
	color.z = math.Min(math.Sqrt(color.z), 1.0)
	return color
}
