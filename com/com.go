package com

import "errors"

func ExpressionCalculate(exp string, lft int, rgt []byte) (bool, error) {
	var lv, fv int = lft, 0
	for i := len(rgt); i > 0; i-- {
		fv = fv + int(rgt[i])<<((len(rgt)-i)*8)
	}

	if exp == `>` {
		if lv > fv {
			return true, nil
		}
		return false, nil
	} else if exp == `>=` {
		if lv >= fv {
			return true, nil
		}
		return false, nil
	} else if exp == `<` {
		if lv < fv {
			return true, nil
		}
		return false, nil
	} else if exp == `<=` {
		if lv <= fv {
			return true, nil
		}
		return false, nil
	} else if exp == `!=` {
		if lv != fv {
			return true, nil
		}
		return false, nil
	} else if exp == `=` {
		if lv == fv {
			return true, nil
		}
		return false, nil
	} else {
		return false, errors.New(`invalid expression`)
	}

}
