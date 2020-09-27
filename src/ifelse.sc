

// if else create functions

def res(age:Int) ={
  if(age>18) "you are major" else "you are minor"
  //nested if else
  if(age>0 && age<13) "you are kid"
  else if (age>=13 && age <=18) "you are minor"
  else if (age>18 && age<40) "you are youth"
  else if(age>=40 && age<=60) "you are aged"
  else if (age>60 && age<100) "you are oldaged"
  else "pls check ur input"
}

//match its like switch
//val nm = "venu"
def test(nm:String) ={
  if(nm=="venu") s"good morning $nm pls do admin tasks"
  else if (nm=="raja") "focus on testing"
  else if (nm=="roja") "focus on development"
  else if (nm=="kuja") "focus on reporting"
  else "you are not member in this project"
}

// mach
// => means do something action

def result(nm:String) = nm match {
  case "venu"=> "focus on admin"
  case "srinu" => "focus on devops"
  case "roja"=> "focus on development"
  case "raja"=> "focus on testing"
  case "kuja"=> "focus on reporting"
  case _ => "you are not member in this project"
}

// match usecase
//val day = "WED"
def days(day:String) = day.toLowerCase match {
  case "sun"| "sunday" | "sat" | "saturday" => "weekend offers"
  case "mon"|"monday" | "wednesday" | "tuesday"| "tue" | "wed" | "thu" | "thursday" |"friday"| "fri" => "daily offers"
  case _ => day

}
// usecase 3 safe guard
val myage = 19
def ageoffer(myage:Int) = myage match {
  case myage if(myage>=1 && myage<13) => "J&J product 30% off"
  case myage if(myage>=13 && myage<=18) =>"mobiles 30% off"
  case myage if(myage>18 && myage<30) => "30% off on laptops"
  case myage if(myage>=30 && myage<60) => "20% off on insurance payment"
  case x if(x>60 && x<100) => "30% off on health products"
  case _ => "no offers"

}

ageoffer(33) // its reusable
ageoffer(2) // hide logic
ageoffer(44)  // no side-effects
ageoffer(99) // no hard-code

ageoffer(123)