use super::*;

pub fn business_day_count(
    start: Expr,
    end: Expr,
) -> Expr {
    let input = vec![start, end];

    Expr::Function {
        input,
        function: FunctionExpr::BusinessDayCount,
        options: FunctionOptions {
            collect_groups: ApplyOptions::GroupWise,
            cast_to_supertypes: true,
            allow_rename: true,
            ..Default::default()
        },
    }
}
