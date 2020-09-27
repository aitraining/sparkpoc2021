val nums = Array(1,22,3,2,-43,55,-6,89)

nums.map(x=>x*x)
//for(x<- nums) yield x*x
val names = Array("venu katragadda","satya","priya","prakash")
names.map(x=>x.toUpperCase())
//for(x<-names) yield x.toUpperCase
// what is map?
//a method that apply a logic/function on top of each and every element
//in map: number of input and output must be same


//filter
//based on true/false/boolean value apply a logic/function of top of each and every element
//input and output number of lements may may not same
nums.filter(x=>x>50)
names.filter(x=>(!x.contains(" ")))
names.filter(x=>x.contains(" "))
names.filter(x=>x.length>10)
names.filter(x=>x.endsWith("a"))
names.filter(x=>x.startsWith("p"))

//flatmap
//map+flatten
//apply a logic/function on top of each and every element next flatter the results
//length of input/output elements not same.
//5 elements...o/p 5,10 elements also
val nam = Array("venu test","narendra modi","rahul gandhi")
val t = nam.map(x=>x.split(" "))
t(0)
nam.map(x=>x.split(" ")).flatten
//instead of use map + flatter use flatmap
val y = nam.flatMap(x=>x.split(" "))
y(0)