package worker

const GoFunc_Add = "add"
const GoFuncKwargs_Add = "add-kwargs"
const PyFunc_Sub = "tasks.subtract"

func Add(x int, y int) int {
	return x + y
}

type adder struct {
}

type adderArgs struct {
	x int
	y int
}

func (a *adder) ParseKwargs(kwargs map[string]interface{}) (interface{}, error) {
	x := kwargs["x"].(float64)
	y := kwargs["y"].(float64)
	return &adderArgs{int(x), int(y)}, nil
}

func (a *adder) RunTask(input interface{}) (interface{}, error) {
	cast := input.(*adderArgs)
	return cast.x + cast.y, nil
}
