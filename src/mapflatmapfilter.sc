val arr = Array(12,23,3,94,55,43,2,9,4,-32)
arr.map(x=>x*x)
val sqr= (x:Int) =>x*x
arr.map(sqr)

// => means do something action
//any method must use x=>x
//it means take one element do something action in that way repeat all elements
// map: a method it applies a function/logic on top of each and every element
// in map how many input elements you have , same number of output elements you will get
//filter
//a function/logic apply on top of each element based on boolean value
arr.filter(x=>x>49)