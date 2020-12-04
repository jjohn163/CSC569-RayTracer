package main

import "fmt"
import "math"

type vec3 struct{
	x float64
	y float64
	z float64
}

func (v vec3) divide(d float64) vec3 {
	return vec3{v.x/d, v.y/d, v.z/d}
}

func (v vec3) multiply(d float64) vec3 {
	return vec3{v.x*d,v.y*d,v.z*d}
}

func (v vec3) multiplyVec3(w vec3) vec3 {
	return vec3{v.x*w.x,v.y*w.y,v.z*w.z}
}

func (v vec3) add(w vec3) vec3 {
	return vec3{v.x + w.x, v.y + w.y, v.z + w.z}
}

func (v vec3) dot(w vec3) float64 {
	return v.x*w.x + v.y*w.y + v.z*w.z
}

func (v vec3) cross(w vec3) vec3 {
	return vec3{v.y*w.z - v.z*w.y, v.z*w.x - v.x*w.z, v.x*w.y - v.y*w.x}
}

func (v vec3) length() float64 {
	return math.Sqrt(v.x*v.x + v.y*v.y + v.z*v.z)
}

func (v vec3) normalize() vec3 {
	return v.divide(v.length())
}
func (v vec3) print() {
	fmt.Printf("%f, %f, %f\n", v.x, v.y, v.z)
}

func (v vec3) toStringColor() string {
	return fmt.Sprintf("%f %f %f ", v.x * 255, v.y * 255, v.z * 255)
}
