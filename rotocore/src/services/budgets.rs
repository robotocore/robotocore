//! AWS Budgets backend (JSON protocol, target `AWSBudgetServiceGateway`).
//!
//! Port of moto's budgets: budget CRUD with normalized Budget dicts
//! (CalculatedSpend/CostTypes/TimePeriod defaults), notifications,
//! subscribers and budget actions.

use crate::store;
use crate::{Request, Response};
use once_cell::sync::Lazy;
use serde_json::{json, Value};

type NotificationKey = (Value, Value);

type BudgetEntry = (String, String, Value, Vec<NotificationKey>);

#[derive(Default)]
struct State {
    /// (account, name) -> {budget dict, notifications}
    budgets: Vec<BudgetEntry>,
    /// (account, budget_name, action dict)
    actions: Vec<(String, String, Value)>,
    /// arn -> {Key: Value}
    tags: Vec<(String, Value)>,
}

fn state() -> store::Slot<State> {
    static SLOT: Lazy<store::Slot<State>> = Lazy::new(|| store::slot("budgets"));
    SLOT.clone()
}

fn ok(v: &Value) -> Response {
    Response::json(200, serde_json::to_vec(v).unwrap_or_default())
}

fn error(status: u16, code: &str, message: &str) -> Response {
    let e = json!({"__type": code, "message": message});
    Response::json(status, serde_json::to_vec(&e).unwrap_or_default())
}

fn not_found(msg: &str) -> Response {
    error(400, "NotFoundException", msg)
}

pub fn dispatch(req: &Request) -> Option<Response> {
    if req.service != "budgets" {
        return None;
    }
    let body = crate::proto::parse_json_body(&req.body);
    let slot = state();
    let mut st = slot.write();
    let b = st.entry((req.account, req.region.clone())).or_default();
    Some(handle(req, b, &body))
}

impl State {
    fn find(&self, account: &str, name: &str) -> Option<usize> {
        self.budgets
            .iter()
            .position(|(a, n, _, _)| a == account && n == name)
    }

    /// moto's Budget.to_dict() normalization.
    fn budget_dict(&self, idx: usize) -> Value {
        let (_, name, budget, _) = &self.budgets[idx];
        let mut out = budget.clone();
        if !out.as_object().unwrap().contains_key("CalculatedSpend") {
            out["CalculatedSpend"] = json!({
                "ActualSpend": {"Amount": "0", "Unit": "USD"},
                "ForecastedSpend": {"Amount": "0", "Unit": "USD"},
            });
        }
        if out["BudgetType"] == json!("COST") && !out.as_object().unwrap().contains_key("CostTypes")
        {
            out["CostTypes"] = json!({
                "IncludeCredit": true, "IncludeDiscount": true,
                "IncludeOtherSubscription": true, "IncludeRecurring": true,
                "IncludeRefund": true, "IncludeSubscription": true,
                "IncludeSupport": true, "IncludeTax": true,
                "IncludeUpfront": true, "UseAmortized": false,
                "UseBlended": false,
            });
        }
        out["CostFilters"] = out.get("CostFilters").cloned().unwrap_or(json!({}));
        out["TimePeriod"] = out
            .get("TimePeriod")
            .cloned()
            .unwrap_or(json!({"Start": 0i64, "End": 3706473600i64}));
        out["BudgetName"] = json!(name);
        out
    }
}

fn handle(req: &Request, st: &mut State, p: &Value) -> Response {
    let account = p
        .get("AccountId")
        .and_then(Value::as_str)
        .unwrap_or("123456789012")
        .to_string();
    let name = p
        .get("BudgetName")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    match req.operation.as_str() {
        "CreateBudget" => {
            let budget = p.get("Budget").cloned().unwrap_or(json!({}));
            let bname = budget
                .get("BudgetName")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            if st.find(&account, &bname).is_some() {
                return error(
                    400,
                    "DuplicateRecordException",
                    &format!("Duplicate record budget {bname}"),
                );
            }
            st.budgets.push((account.clone(), bname, budget, vec![]));
            ok(&json!({}))
        }
        "DescribeBudget" => match st.find(&account, &name) {
            Some(idx) => {
                let dict = st.budget_dict(idx);
                ok(&json!({"Budget": dict}))
            }
            None => not_found(&format!(
                "Unable to get budget: {name} - no budget with name {name} was found. "
            )),
        },
        "DescribeBudgets" => {
            let out: Vec<Value> = st
                .budgets
                .iter()
                .enumerate()
                .filter(|(_, (a, _, _, _))| a == &account)
                .map(|(idx, _)| st.budget_dict(idx))
                .collect();
            ok(&json!({"Budgets": out}))
        }
        "UpdateBudget" => {
            let new_budget = p.get("NewBudget").cloned().unwrap_or(json!({}));
            let bname = new_budget
                .get("BudgetName")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            match st.find(&account, &bname) {
                Some(idx) => {
                    let (stored, notifs) = {
                        let (_, _, b, n) = &st.budgets[idx];
                        (b.clone(), n.clone())
                    };
                    let mut merged = stored;
                    if let Some(obj) = new_budget.as_object() {
                        for (k, v) in obj {
                            merged[k.clone()] = v.clone();
                        }
                    }
                    merged["LastUpdatedTime"] = json!(crate::proto::now_unix());
                    st.budgets[idx] = (account.clone(), bname, merged, notifs);
                    ok(&json!({}))
                }
                None => not_found(&format!(
                    "Unable to update budget: {bname} - the budget doesn't exist."
                )),
            }
        }
        "DeleteBudget" => match st.find(&account, &name) {
            Some(idx) => {
                st.budgets.remove(idx);
                st.actions.retain(|(_, bn, _)| bn != &name);
                ok(&json!({}))
            }
            None => not_found(&format!(
                "Unable to delete budget: {name} - the budget doesn't exist."
            )),
        },
        "CreateNotification" => {
            if let Some(idx) = st.find(&account, &name) {
                let details = p.get("Notification").cloned().unwrap_or(json!({}));
                let subscribers = p.get("Subscribers").cloned().unwrap_or(json!([]));
                st.budgets[idx].3.push((details, subscribers));
                return ok(&json!({}));
            }
            not_found(&name)
        }
        "DeleteNotification" => {
            if let Some(idx) = st.find(&account, &name) {
                let details = p.get("Notification").cloned().unwrap_or(json!({}));
                st.budgets[idx].3.retain(|(d, _)| *d != details);
                return ok(&json!({}));
            }
            not_found(&name)
        }
        "DescribeNotificationsForBudget" => {
            if let Some(idx) = st.find(&account, &name) {
                let list: Vec<Value> = st.budgets[idx].3.iter().map(|(d, _)| d.clone()).collect();
                return ok(&json!({"Notifications": list}));
            }
            not_found(&name)
        }
        "DescribeSubscribersForNotification" => {
            if let Some(idx) = st.find(&account, &name) {
                let details = p.get("Notification").cloned().unwrap_or(json!({}));
                let subs = st.budgets[idx]
                    .3
                    .iter()
                    .find(|(d, _)| *d == details)
                    .map(|(_, s)| s.clone());
                return ok(&json!({"Subscribers": subs, "Notification": details}));
            }
            not_found(&name)
        }
        "UpdateNotification" => {
            if let Some(idx) = st.find(&account, &name) {
                let oldn = p.get("OldNotification").cloned().unwrap_or(json!({}));
                let newn = p.get("NewNotification").cloned().unwrap_or(json!({}));
                if let Some(entry) = st.budgets[idx].3.iter_mut().find(|(d, _)| *d == oldn) {
                    let subscribers = entry.1.clone();
                    entry.0 = newn.clone();
                    return ok(&json!({
                        "OldNotification": oldn,
                        "NewNotification": newn,
                        "Subscribers": subscribers,
                    }));
                }
            }
            not_found(&name)
        }
        "UpdateSubscriber" => {
            if let Some(idx) = st.find(&account, &name) {
                let notif = p.get("Notification").cloned().unwrap_or(json!({}));
                let old_sub = p.get("OldSubscriber").cloned().unwrap_or(json!({}));
                let new_sub = p.get("NewSubscriber").cloned().unwrap_or(json!({}));
                if let Some(entry) = st.budgets[idx].3.iter_mut().find(|(d, _)| *d == notif) {
                    if let Some(list) = entry.1.as_array_mut() {
                        for s in list.iter_mut() {
                            if *s == old_sub {
                                *s = new_sub.clone();
                            }
                        }
                    }
                    return ok(&json!({
                        "Notification": notif,
                        "OldSubscriber": old_sub,
                        "NewSubscriber": new_sub,
                    }));
                }
            }
            not_found(&name)
        }
        "CreateBudgetAction" => {
            if st.find(&account, &name).is_none() {
                return not_found(&format!(
                    "Unable to get budget: {name} - the budget doesn't exist."
                ));
            }
            let action = json!({
                "ActionId": uuid::Uuid::new_v4(),
                "BudgetName": name,
                "NotificationType": p.get("NotificationType").cloned().unwrap_or(Value::Null),
                "ActionType": p.get("ActionType").cloned().unwrap_or(Value::Null),
                "ActionThreshold": p.get("ActionThreshold").cloned().unwrap_or(json!({})),
                "Definition": p.get("Definition").cloned().unwrap_or(json!({})),
                "ExecutionRoleArn": p.get("ExecutionRoleArn").cloned().unwrap_or(Value::Null),
                "ApprovalModel": p.get("ApprovalModel").cloned().unwrap_or(Value::Null),
                "Subscribers": p.get("Subscribers").cloned().unwrap_or(json!([])),
                "Status": "STANDBY",
            });
            let action_id = action["ActionId"].as_str().unwrap_or("").to_string();
            st.actions.push((account.clone(), name.clone(), action));
            ok(&json!({"ActionId": action_id}))
        }
        "DescribeBudgetAction" => {
            let action_id = p.get("ActionId").and_then(Value::as_str).unwrap_or("");
            match st
                .actions
                .iter()
                .find(|(a, _, ac)| a == &account && ac["ActionId"] == json!(action_id))
            {
                Some((_, _, ac)) => ok(&json!({"Action": ac.clone(), "Account": &account})),
                None => not_found(&format!(
                    "Unable to get action: {action_id} - the action doesn't exist."
                )),
            }
        }
        "UpdateBudgetAction" => {
            let action_id = p.get("ActionId").and_then(Value::as_str).unwrap_or("");
            if let Some((_, _, ac)) = st
                .actions
                .iter_mut()
                .find(|(a, _, ac)| a == &account && ac["ActionId"] == json!(action_id))
            {
                if let Some(nt) = p.get("NotificationType") {
                    ac["NotificationType"] = nt.clone();
                }
                if let Some(at) = p.get("ActionThreshold") {
                    ac["ActionThreshold"] = at.clone();
                }
                if let Some(d) = p.get("Definition") {
                    ac["Definition"] = d.clone();
                }
                return ok(&json!({"NewAction": ac.clone(), "Account": &account}));
            }
            not_found(&format!(
                "Unable to get action: {action_id} - the action doesn't exist."
            ))
        }
        "ExecuteBudgetAction" => {
            let action_id = p.get("ActionId").and_then(Value::as_str).unwrap_or("");
            if let Some((_, _, ac)) = st
                .actions
                .iter_mut()
                .find(|(a, _, ac)| a == &account && ac["ActionId"] == json!(action_id))
            {
                match p.get("ExecutionType").and_then(Value::as_str).unwrap_or("") {
                    "APPROVE_BUDGET_ACTION" | "RETRY_BUDGET_ACTION" => {
                        ac["Status"] = json!("EXECUTION_SUCCESS")
                    }
                    "REVERSE_BUDGET_ACTION" => ac["Status"] = json!("REVERSE_SUCCESS"),
                    "RESET_BUDGET_ACTION" => ac["Status"] = json!("STANDBY"),
                    _ => {}
                }
                return ok(&json!({
                    "AccountId": &account,
                    "BudgetName": name,
                    "ActionId": action_id,
                    "ExecutionType": p.get("ExecutionType").cloned().unwrap_or(Value::Null),
                }));
            }
            not_found(&format!(
                "Unable to get action: {action_id} - the action doesn't exist."
            ))
        }
        "CreateSubscriber" => {
            if let Some(idx) = st.find(&account, &name) {
                let notif = p.get("Notification").cloned().unwrap_or(json!({}));
                let subscriber = p.get("Subscriber").cloned().unwrap_or(json!({}));
                if let Some(entry) = st.budgets[idx].3.iter_mut().find(|(d, _)| *d == notif) {
                    if let Some(list) = entry.1.as_array_mut() {
                        list.push(subscriber.clone());
                    }
                    return ok(&json!({"Notification": notif, "Subscriber": subscriber}));
                }
                return not_found(&format!("Unable to find notification for budget {name}"));
            }
            not_found(&name)
        }
        "DeleteSubscriber" => {
            if let Some(idx) = st.find(&account, &name) {
                let notif = p.get("Notification").cloned().unwrap_or(json!({}));
                let subscriber = p.get("Subscriber").cloned().unwrap_or(json!({}));
                if let Some(entry) = st.budgets[idx].3.iter_mut().find(|(d, _)| *d == notif) {
                    if let Some(list) = entry.1.as_array_mut() {
                        list.retain(|s| *s != subscriber);
                    }
                    return ok(&json!({"Notification": notif, "Subscriber": subscriber}));
                }
                return not_found(&format!("Unable to find notification for budget {name}"));
            }
            not_found(&name)
        }
        "DeleteBudgetAction" => {
            let action_id = p.get("ActionId").and_then(Value::as_str).unwrap_or("");
            let before = st.actions.len();
            st.actions
                .retain(|(a, _, ac)| !(a == &account && ac["ActionId"] == json!(action_id)));
            if before == st.actions.len() {
                return not_found(&format!(
                    "Unable to get action: {action_id} - the action doesn't exist."
                ));
            }
            ok(&json!({"Account": &account, "ActionId": action_id}))
        }
        "DescribeBudgetActionsForAccount" => {
            let list: Vec<Value> = st
                .actions
                .iter()
                .filter(|(a, _, _)| a == &account)
                .map(|(_, _, ac)| ac.clone())
                .collect();
            ok(&json!({"Actions": list}))
        }
        "DescribeBudgetActionsForBudget" => {
            let list: Vec<Value> = st
                .actions
                .iter()
                .filter(|(a, _, ac)| a == &account && ac["BudgetName"] == json!(name))
                .map(|(_, _, ac)| ac.clone())
                .collect();
            ok(&json!({"Actions": list}))
        }
        "DescribeBudgetActionHistories" => ok(&json!({"ActionHistories": []})),
        "DescribeBudgetNotificationsForAccount" => {
            ok(&json!({"BudgetNotificationsForAccount": []}))
        }
        "DescribeBudgetPerformanceHistory" => {
            ok(&json!({"BudgetPerformanceHistory": {"BudgetName": name}}))
        }
        "ListTagsForResource" => {
            let arn = p.get("ResourceARN").and_then(Value::as_str).unwrap_or("");
            let list = tag_list(st, arn);
            ok(&json!({"ResourceTags": list}))
        }
        "TagResource" => {
            let arn = p.get("ResourceARN").and_then(Value::as_str).unwrap_or("");
            if let Some(tags) = p.get("ResourceTags") {
                store_tags(st, arn, tags);
            }
            ok(&json!({}))
        }
        "UntagResource" => {
            let arn = p.get("ResourceARN").and_then(Value::as_str).unwrap_or("");
            let keys: Vec<String> = p
                .get("ResourceTagKeys")
                .or_else(|| p.get("ResourceKeys"))
                .or_else(|| p.get("TagKeys"))
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default();
            for (a, v) in st.tags.iter_mut() {
                if a == arn {
                    if let Some(obj) = v.as_object_mut() {
                        for k in &keys {
                            obj.remove(k);
                        }
                    }
                }
            }
            ok(&json!({}))
        }
        _ => error(
            501,
            "NotImplemented",
            &format!("The {} action has not been implemented", req.operation),
        ),
    }
}

fn tag_list(st: &State, arn: &str) -> Vec<Value> {
    st.tags
        .iter()
        .find(|(a, _)| a == arn)
        .map(|(_, v)| v.clone())
        .unwrap_or(json!({}))
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| json!({"Key": k, "Value": v}))
                .collect()
        })
        .unwrap_or_default()
}

fn store_tags(st: &mut State, arn: &str, tags: &Value) {
    let entry = match st.tags.iter_mut().find(|(a, _)| a == arn) {
        Some((_, v)) => v,
        None => {
            st.tags.push((arn.to_string(), json!({})));
            let i = st.tags.len() - 1;
            &mut st.tags[i].1
        }
    };
    for tag in tags.as_array().unwrap_or(&vec![]) {
        let key = tag.get("Key").and_then(Value::as_str).unwrap_or("");
        if key.is_empty() {
            continue;
        }
        entry.as_object_mut().unwrap().insert(
            key.to_string(),
            tag.get("Value").cloned().unwrap_or(Value::Null),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn req(op: &str, p: Value) -> Request {
        Request {
            service: "budgets".into(),
            operation: op.into(),
            method: "POST".into(),
            path: "/".into(),
            query_string: String::new(),
            headers: Default::default(),
            body: bytes::Bytes::from(serde_json::to_vec(&p).unwrap_or_default()),
            account: 123456789012,
            region: "us-east-1".into(),
        }
    }

    #[test]
    fn budget_and_notification_flow() {
        let slot = state();
        let mut guard = slot.write();
        let b = guard.entry((123456789012, "us-east-1".into())).or_default();
        let create = req(
            "CreateBudget",
            json!({
                "AccountId": "123456789012",
                "Budget": {"BudgetName": "b1", "BudgetLimit": {"Amount": "100", "Unit": "USD"},
                            "TimeUnit": "MONTHLY", "BudgetType": "COST"},
            }),
        );
        let created = handle(&create, b, &serde_json::from_slice(&create.body).unwrap());
        assert_eq!(created.status, 200);

        let dup = req(
            "CreateBudget",
            json!({"AccountId": "123456789012", "Budget": {"BudgetName": "b1"}}),
        );
        let dupe = handle(&dup, b, &serde_json::from_slice(&dup.body).unwrap());
        assert_eq!(
            serde_json::from_slice::<Value>(&dupe.body).unwrap()["__type"],
            "DuplicateRecordException"
        );

        let notif = req(
            "CreateNotification",
            json!({
                "AccountId": "123456789012",
                "BudgetName": "b1",
                "Notification": {"NotificationType": "ACTUAL", "Threshold": 80.0},
                "Subscribers": [{"SubscriptionType": "EMAIL", "Address": "x@y.z"}],
            }),
        );
        handle(&notif, b, &serde_json::from_slice(&notif.body).unwrap());
        let describe = req(
            "DescribeNotificationsForBudget",
            json!({"AccountId": "123456789012", "BudgetName": "b1"}),
        );
        let resp = handle(
            &describe,
            b,
            &serde_json::from_slice(&describe.body).unwrap(),
        );
        let resp = serde_json::from_slice::<Value>(&resp.body).unwrap();
        assert_eq!(resp["Notifications"].as_array().unwrap().len(), 1);
        assert_eq!(resp["Notifications"][0]["NotificationType"], "ACTUAL");

        let upd = req(
            "UpdateBudget",
            json!({
                "AccountId": "123456789012",
                "NewBudget": {"BudgetName": "b1", "BudgetLimit": {"Amount": "200", "Unit": "USD"}, "BudgetType": "COST"},
            }),
        );
        handle(&upd, b, &serde_json::from_slice(&upd.body).unwrap());
        let describe = req(
            "DescribeBudget",
            json!({"AccountId": "123456789012", "BudgetName": "b1"}),
        );
        let resp = handle(
            &describe,
            b,
            &serde_json::from_slice(&describe.body).unwrap(),
        );
        let resp = serde_json::from_slice::<Value>(&resp.body).unwrap();
        assert_eq!(resp["Budget"]["BudgetLimit"]["Amount"], "200");
    }
}
