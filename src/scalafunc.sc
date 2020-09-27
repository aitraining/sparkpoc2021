def func(x:Int) = x*x

func(15) //calling functions
//function hide logic
//its re-usable ...
//no side-effects.

//recursive functin
//a functin call itself (a function call within the same function)
//return datatype mandatory in recursive functions

def fact(n:Int):Int = n match {
  case x if(x<=1) =>1
  case _ => n*fact(n-1)
}
//5 fact
1*2*3*4*5
 fact(5)
//5*fact(5-1)
//5*4* 3*2*1

def power(b:Int, p:Int):Int = p match {
  case x if(x<1)=>1
  case _ => b * power(b,p-1)
}

def pow (b:Int, p:Int):Int = {
  if(p <1) 1
  else b * pow(b,p-1)
}

power(2,4)
2*2*2*2
power (4,-1)

//nested functions
//a function call in another function.
//returning datatype optional
def maxnum(x:Int, y:Int, z:Int) = {
  def max(x:Int, y:Int) = if(x>y) x else y
  max(x,max(y,z))
}
maxnum(4,99,-101)

// anonymous function
// a function without define as function (fun)
// => means do something action
val mul = (x:Int, y:Int)=> x*y
val upper = (fn:String, ln:String )=> fn.toUpperCase + " "+ln.toUpperCase
upper("venu","testing")
mul(9,2)



/// Higher order function
// a function call in another function as a parameter
def sqr (x:Int) = x*x
def cub (x:Int) = x*x*x

def hof(res: (Int, Int) => Int,x:Int, y:Int) = {
  res(x,y)
}

def hof2(res:Int=>Int, x:Int)  = {
  res(x)
}
def twohof(x:Int => Int, y:Int=>Int, a:Int) = {
  x(a) * y(a)
}
twohof(sqr,cub,9)

hof2(fact,5)
hof2(cub,3)
hof2(sqr,9)

hof(mul,4,9)