//! Go's `math` functions as Go 1.27 runs them on linux/amd64, ported so that
//! [`super::from_otlp`] reproduces the Datadog translator's floating-point results bit for bit.
//!
//! Rust's `ln`, `exp`, and `powf` call the platform's libm, which rounds differently from Go in
//! the last place, and one ulp moves a bin's count in `InsertInterpolate` and a count's rounding
//! in `ChangeMapping`. Go itself differs by architecture: on amd64, `Log` and `Exp` are assembly
//! (`math/log_amd64.s`, `math/exp_amd64.s`), and `Exp` takes a fused multiply-add path on a CPU
//! with AVX and FMA, which is the path ported here (`f64::mul_add` is fused on every target). On
//! arm64, Go runs the pure-Go `Log` and its own `Exp` assembly, and its compiler fuses
//! multiply-adds, so a Collector on arm64 can disagree with these results in the last place.
//! `testdata/differential/datadog-sketch/primitives.json` pins every function here against Go.
//!
//! `Pow`, `Frexp`, and `Ldexp` are pure Go on amd64, and Go's compiler doesn't fuse their
//! arithmetic at `GOAMD64=v1`, the default.

// The constants are Go's own literals, as written in `math`'s sources, so a reader can compare
// them; each parses to the same `f64` the shorter form would.
#![allow(clippy::excessive_precision, clippy::approx_constant)]

const SIGN: u64 = 1 << 63;
const POS_INF: u64 = 0x7FF0_0000_0000_0000;
const NEG_INF: u64 = 0xFFF0_0000_0000_0000;
const SMALLEST_NORMAL: f64 = 2.2250738585072014e-308;

/// Go's float64-to-int conversion on amd64 (`CVTTSD2SQ`): truncation, and `i64::MIN` for `NaN`
/// or a value out of range, where Rust's `as` saturates.
pub(crate) fn to_int(x: f64) -> i64 {
    if x.is_nan() || x >= 9_223_372_036_854_775_808.0 || x < -9_223_372_036_854_775_808.0 {
        i64::MIN
    } else {
        x as i64
    }
}

/// `math.Log`, from `log_amd64.s`.
pub(crate) fn log(x: f64) -> f64 {
    const HSQRT2: f64 = 7.07106781186547524401e-01;
    const LN2_HI: f64 = 6.93147180369123816490e-01;
    const LN2_LO: f64 = 1.90821492927058770002e-10;
    const L1: f64 = 6.666666666666735130e-01;
    const L2: f64 = 3.999999999940941908e-01;
    const L3: f64 = 2.857142874366239149e-01;
    const L4: f64 = 2.222219843214978396e-01;
    const L5: f64 = 1.818357216161805012e-01;
    const L6: f64 = 1.531383769920937332e-01;
    const L7: f64 = 1.479819860511658591e-01;

    let bits = x.to_bits();
    if bits & !SIGN == 0 {
        return f64::NEG_INFINITY;
    }
    if (bits as i64) < 0 {
        return f64::from_bits(0x7FF8_0000_0000_0001);
    }
    if bits >= POS_INF {
        return x;
    }
    // The assembly's own frexp: it reads the exponent field as is, so a subnormal `x` isn't
    // normalized first, unlike `math.Frexp`.
    let mut f1 = f64::from_bits((bits & 0x000F_FFFF_FFFF_FFFF) | 0.5f64.to_bits());
    let mut k = (((bits >> 52) & 0x7FF) as i32 - 0x3FE) as f64;
    // `CMPSD $5` is not-less-than: the assembly takes this branch at `f1 == HSQRT2` too, where
    // `log.go` doesn't.
    if f1 <= HSQRT2 {
        k -= 1.0;
        f1 *= 2.0;
    }
    let f = f1 - 1.0;
    let s = f / (2.0 + f);
    let s2 = s * s;
    let s4 = s2 * s2;
    let t1 = s2 * (((L7 * s4 + L5) * s4 + L3) * s4 + L1);
    let t2 = s4 * ((L6 * s4 + L4) * s4 + L2);
    let r = t1 + t2;
    let hfsq = 0.5 * f * f;
    k * LN2_HI - ((hfsq - (s * (hfsq + r) + k * LN2_LO)) - f)
}

/// `math.Exp`, from `exp_amd64.s`'s AVX/FMA path.
pub(crate) fn exp(x: f64) -> f64 {
    const LOG2E: f64 = 1.4426950408889634073599246810018920;
    const LN2U: f64 = 0.69314718055966295651160180568695068359375;
    const LN2L: f64 = 0.28235290563031577122588448175013436025525412068e-12;
    const OVERFLOW: f64 = 7.09782712893384e+02;
    const C: [f64; 7] = [
        2.4801587301587301587e-5,
        1.9841269841269841270e-4,
        1.3888888888888888889e-3,
        8.3333333333333333333e-3,
        4.1666666666666666667e-2,
        1.6666666666666666667e-1,
        0.5,
    ];

    let bits = x.to_bits();
    if bits & !SIGN >= POS_INF {
        return if bits == NEG_INF { 0.0 } else { x };
    }
    if x > OVERFLOW {
        return f64::INFINITY;
    }
    // `CVTSD2SL`: round to nearest even, and `i32::MIN` when out of range.
    let rounded = (LOG2E * x).round_ties_even();
    let e = if (-2_147_483_648.0..2_147_483_648.0).contains(&rounded) {
        rounded as i32
    } else {
        i32::MIN
    };
    let k = e as f64;
    let mut r = (-k).mul_add(LN2U, x);
    r = (-k).mul_add(LN2L, r);
    r *= 0.0625;
    let mut p = C[0];
    for c in &C[1..] {
        p = p.mul_add(r, *c);
    }
    p = p.mul_add(r, 1.0);
    r *= p;
    for _ in 0..3 {
        r *= r + 2.0;
    }
    r = (r + 2.0).mul_add(r, 1.0);

    // `fr * 2**e`, with the assembly's own denormal handling.
    let biased = e.wrapping_add(0x3FF);
    let (scale, biased) = if biased <= 0 {
        if biased < -52 {
            return 0.0;
        }
        let first = f64::from_bits(((biased + 0x3FE) as u32 as u64) << 52);
        (first, 1)
    } else if biased >= 0x7FF {
        return f64::INFINITY;
    } else {
        (1.0, biased)
    };
    r *= scale;
    r * f64::from_bits((biased as u32 as u64) << 52)
}

/// Go's `normalize`: a normal number and exponent for a finite, non-zero `x`.
fn normalize(x: f64) -> (f64, i64) {
    if x.abs() < SMALLEST_NORMAL {
        (x * (1u64 << 52) as f64, -52)
    } else {
        (x, 0)
    }
}

/// `math.Frexp`.
pub(crate) fn frexp(f: f64) -> (f64, i64) {
    if f == 0.0 || f.is_infinite() || f.is_nan() {
        return (f, 0);
    }
    let (f, mut exp) = normalize(f);
    let mut x = f.to_bits();
    exp += ((x >> 52) & 0x7FF) as i64 - 1023 + 1;
    x &= !(0x7FF << 52);
    x |= 1022 << 52;
    (f64::from_bits(x), exp)
}

/// `math.Ldexp`.
pub(crate) fn ldexp(frac: f64, exp: i64) -> f64 {
    if frac == 0.0 || frac.is_infinite() || frac.is_nan() {
        return frac;
    }
    let (frac, e) = normalize(frac);
    let mut exp = exp + e;
    let mut x = frac.to_bits();
    exp += ((x >> 52) & 0x7FF) as i64 - 1023;
    if exp < -1075 {
        return 0.0f64.copysign(frac);
    }
    if exp > 1023 {
        return if frac < 0.0 { f64::NEG_INFINITY } else { f64::INFINITY };
    }
    let mut m = 1.0;
    if exp < -1022 {
        exp += 53;
        m = 1.0 / (1u64 << 53) as f64;
    }
    x &= !(0x7FF << 52);
    x |= ((exp + 1023) as u64) << 52;
    m * f64::from_bits(x)
}

fn modf(f: f64) -> (f64, f64) {
    let int = f.trunc();
    (int, (f - int).copysign(f))
}

fn is_odd_int(x: f64) -> bool {
    if x.abs() >= (1u64 << 53) as f64 {
        return false;
    }
    let (xi, xf) = modf(x);
    xf == 0.0 && (xi as i64) & 1 == 1
}

/// `math.Pow`.
pub(crate) fn pow(x: f64, y: f64) -> f64 {
    if y == 0.0 || x == 1.0 {
        return 1.0;
    }
    if y == 1.0 {
        return x;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 {
        if y < 0.0 {
            return if x.is_sign_negative() && is_odd_int(y) {
                f64::NEG_INFINITY
            } else {
                f64::INFINITY
            };
        }
        return if x.is_sign_negative() && is_odd_int(y) { x } else { 0.0 };
    }
    if y.is_infinite() {
        return if x == -1.0 {
            1.0
        } else if (x.abs() < 1.0) == (y > 0.0) {
            0.0
        } else {
            f64::INFINITY
        };
    }
    if x.is_infinite() {
        if x < 0.0 {
            return pow(1.0 / x, -y);
        }
        return if y < 0.0 { 0.0 } else { f64::INFINITY };
    }
    if y == 0.5 {
        return x.sqrt();
    }
    if y == -0.5 {
        return 1.0 / x.sqrt();
    }

    let (mut yi, mut yf) = modf(y.abs());
    if yf != 0.0 && x < 0.0 {
        return f64::NAN;
    }
    if yi >= 9_223_372_036_854_775_808.0 {
        return if x == -1.0 {
            1.0
        } else if (x.abs() < 1.0) == (y > 0.0) {
            0.0
        } else {
            f64::INFINITY
        };
    }

    let mut a1 = 1.0;
    let mut ae: i64 = 0;
    if yf != 0.0 {
        if yf > 0.5 {
            yf -= 1.0;
            yi += 1.0;
        }
        a1 = exp(yf * log(x));
    }

    let (mut x1, mut xe) = frexp(x);
    let mut i = yi as i64;
    while i != 0 {
        if !(-(1 << 12)..=(1 << 12)).contains(&xe) {
            ae += xe;
            break;
        }
        if i & 1 == 1 {
            a1 *= x1;
            ae += xe;
        }
        x1 *= x1;
        xe <<= 1;
        if x1 < 0.5 {
            x1 += x1;
            xe -= 1;
        }
        i >>= 1;
    }

    if y < 0.0 {
        a1 = 1.0 / a1;
        ae = -ae;
    }
    ldexp(a1, ae)
}

/// `math.Max`: `+Inf` wins, then `NaN`, and `+0` over `-0`; Rust's `f64::max` ignores a `NaN`.
pub(crate) fn max(x: f64, y: f64) -> f64 {
    if x == f64::INFINITY || y == f64::INFINITY {
        return f64::INFINITY;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 && x == y {
        return if x.is_sign_negative() { y } else { x };
    }
    if x > y {
        x
    } else {
        y
    }
}

/// `math.Min`, the mirror of [`max`].
pub(crate) fn min(x: f64, y: f64) -> f64 {
    if x == f64::NEG_INFINITY || y == f64::NEG_INFINITY {
        return f64::NEG_INFINITY;
    }
    if x.is_nan() || y.is_nan() {
        return f64::NAN;
    }
    if x == 0.0 && x == y {
        return if x.is_sign_negative() { x } else { y };
    }
    if x < y {
        x
    } else {
        y
    }
}
