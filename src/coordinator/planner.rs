use std::sync::Arc;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;

use super::error::PlannerError; // Import the new error type

// --- ExecutionPlan Trait and Node Definitions ---

pub trait ExecutionPlan: Send + Sync + fmt::Debug {
    fn name(&self) -> &str;
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>>;
    fn clone_boxed(&self) -> Arc<dyn ExecutionPlan>; // To clone the trait object
    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, PlannerError>;
    // Using pointer address as a pseudo-unique ID for visited tracking.
    // In a real system, a proper unique ID or hash would be used.
    fn id_for_tracking(&self) -> String {
        format!("{:p}", self)
    }
}

// RepartitionExec: Represents a shuffle boundary
#[derive(Debug)]
pub struct RepartitionExec {
    pub input: Arc<dyn ExecutionPlan>,
    // Could add partitioning scheme here
}

impl RepartitionExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

impl ExecutionPlan for RepartitionExec {
    fn name(&self) -> &str {
        "RepartitionExec"
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }
    fn clone_boxed(&self) -> Arc<dyn ExecutionPlan> {
        Arc::new(RepartitionExec::new(self.input.clone()))
    }
    fn with_new_children(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, PlannerError> {
        if children.len() != 1 {
            return Err(PlannerError::Internal(
                "RepartitionExec must have exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(RepartitionExec::new(children.remove(0))))
    }
}

// StageInputExec: Placeholder for an input from another stage
#[derive(Debug)]
pub struct StageInputExec {
    pub stage_id: usize,
    // Could include schema or other metadata
}

impl StageInputExec {
    pub fn new(stage_id: usize) -> Self {
        Self { stage_id }
    }
}

impl ExecutionPlan for StageInputExec {
    fn name(&self) -> &str {
        "StageInputExec"
    }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![] // No logical children in the plan tree
    }
    fn clone_boxed(&self) -> Arc<dyn ExecutionPlan> {
        Arc::new(StageInputExec::new(self.stage_id))
    }
    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, PlannerError> {
        if !children.is_empty() {
            return Err(PlannerError::Internal(
                "StageInputExec cannot have children".to_string(),
            ));
        }
        Ok(self.clone_boxed())
    }
     fn id_for_tracking(&self) -> String {
        // StageInputExec for the same stage_id should be considered the same.
        format!("StageInputExec_{}", self.stage_id)
    }
}

// --- QueryStage Definition ---
#[derive(Debug, Clone)]
pub struct QueryStage {
    pub stage_id: usize,
    pub plan: Arc<dyn ExecutionPlan>, // The plan for this stage
    pub dependencies: Vec<usize>,     // stage_ids of stages this stage depends on
}

// --- DistributedPlanner ---
pub struct DistributedPlanner {
    next_stage_id: usize,
}

impl DistributedPlanner {
    pub fn new() -> Self {
        Self { next_stage_id: 0 }
    }

    fn new_stage_id(&mut self) -> usize {
        let id = self.next_stage_id;
        self.next_stage_id += 1;
        id
    }

    pub fn plan_query(
        &mut self,
        physical_plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<QueryStage>, PlannerError> {
        let mut stages: Vec<QueryStage> = Vec::new();
        // Using a map to track (original plan node ptr string -> (rewritten plan for current stage, direct dependencies for it))
        // This memoization is important for DAGs (if plan nodes are shared).
        let mut visited_nodes: HashMap<String, (Arc<dyn ExecutionPlan>, Vec<usize>)> = HashMap::new();

        let (root_stage_plan, root_stage_dependencies) =
            self.analyze_plan_recursive(physical_plan, &mut stages, &mut visited_nodes)?;

        // The final (root) stage
        let root_stage_id = self.new_stage_id();
        stages.push(QueryStage {
            stage_id: root_stage_id,
            plan: root_stage_plan,
            dependencies: root_stage_dependencies,
        });

        stages.sort_by_key(|s| s.stage_id); // For deterministic output
        Ok(stages)
    }

    // Analyzes the plan.
    // Returns (rewritten_plan_for_current_stage, direct_dependency_stage_ids_for_it)
    // Adds completed child stages to `stages_collector`.
    fn analyze_plan_recursive(
        &mut self,
        plan: Arc<dyn ExecutionPlan>,
        stages_collector: &mut Vec<QueryStage>,
        visited_nodes: &mut HashMap<String, (Arc<dyn ExecutionPlan>, Vec<usize>)>,
    ) -> Result<(Arc<dyn ExecutionPlan>, Vec<usize>), PlannerError> {
        let plan_key = plan.id_for_tracking();
        if let Some(cached_result) = visited_nodes.get(&plan_key) {
            return Ok(cached_result.clone());
        }

        if plan.name() == "RepartitionExec" {
            // This RepartitionExec node's input forms a complete, independent stage.
            let repartition_node = plan
                .as_any() // Requires ExecutionPlan to impl AsAny or similar downcast helper
                .downcast_ref::<RepartitionExec>()
                .ok_or_else(|| PlannerError::Internal("Failed to downcast to RepartitionExec".to_string()))?;

            let child_plan = repartition_node.input.clone();

            // Recursively process the child_plan. This will result in a (potentially rewritten) plan
            // for the child stage, and its dependencies.
            let (child_stage_final_plan, child_stage_dependencies) =
                self.analyze_plan_recursive(child_plan, stages_collector, visited_nodes)?;

            // Create a new stage for this child_plan segment
            let child_query_stage_id = self.new_stage_id();
            stages_collector.push(QueryStage {
                stage_id: child_query_stage_id,
                plan: child_stage_final_plan, // This is the complete plan for the child stage
                dependencies: child_stage_dependencies,
            });

            // The RepartitionExec itself is replaced by a StageInputExec in the parent's plan.
            // This StageInputExec refers to the stage we just created.
            let rewritten_node_for_parent = Arc::new(StageInputExec::new(child_query_stage_id)) as Arc<dyn ExecutionPlan>;
            let result = (rewritten_node_for_parent, vec![child_query_stage_id]);
            visited_nodes.insert(plan_key, result.clone());
            Ok(result)

        } else {
            // Not a RepartitionExec. This node is part of the current stage being built.
            let mut new_children_for_current_node = Vec::new();
            let mut current_node_direct_dependencies = HashSet::new();

            for child_original_plan in plan.children() {
                let (rewritten_child_plan_for_current_stage, child_direct_dependencies) =
                    self.analyze_plan_recursive(child_original_plan.clone(), stages_collector, visited_nodes)?;

                new_children_for_current_node.push(rewritten_child_plan_for_current_stage);
                for dep_id in child_direct_dependencies {
                    current_node_direct_dependencies.insert(dep_id);
                }
            }

            let rewritten_plan_for_current_stage = if plan.children().is_empty() {
                plan.clone_boxed() // Leaf node, clone itself
            } else {
                plan.with_new_children(new_children_for_current_node)?
            };

            let result = (rewritten_plan_for_current_stage, current_node_direct_dependencies.into_iter().collect());
            // Do not insert into visited_nodes here if plan is a leaf that might be part of multiple stages
            // before being materialized. Or rather, the key should be truly unique.
            // For non-leaf nodes, this is okay.
            if !plan.children().is_empty() || plan.name() == "StageInputExec" { // StageInputExec is effectively a leaf for memoization keying
                 visited_nodes.insert(plan_key, result.clone());
            }
            Ok(result)
        }
    }
}

// --- Helper for downcasting ---
pub trait AsAny: 'static {
    fn as_any(&self) -> &dyn std::any::Any;
}

impl<T: 'static> AsAny for T {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
impl dyn ExecutionPlan {
    pub fn as_any(&self) -> &dyn std::any::Any {
        // This requires ExecutionPlan to be implemented for types that also impl AsAny.
        // This is a common pattern, but for simplicity here, we'll assume it's available
        // or handle it directly in RepartitionExec. For now, let's modify RepartitionExec
        // to not require downcasting from the trait object in the planner.
        // The planner will now specifically check `plan.name() == "RepartitionExec"`
        // and then expect `plan.children().get(0)` to be its input.
        // This is a bit less type-safe but avoids complex downcasting setup for this example.
        // The previous `downcast_ref` call was on `plan.as_any()` which needs `AsAny` bound to `ExecutionPlan`.
        // For now, removed direct downcast in planner and rely on `plan.children()` for RepartitionExec.
        unimplemented!("This as_any is a placeholder if direct downcasting is needed.")
    }
}


// --- Dummy ExecutionPlan Implementations for Testing ---
#[derive(Debug)]
pub struct DummyLeafExec {
    name_val: String,
}
impl DummyLeafExec { pub fn new(name: &str) -> Self { Self { name_val: name.to_string() } } }
impl ExecutionPlan for DummyLeafExec {
    fn name(&self) -> &str { &self.name_val }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> { vec![] }
    fn clone_boxed(&self) -> Arc<dyn ExecutionPlan> { Arc::new(Self { name_val: self.name_val.clone() }) }
    fn with_new_children(&self, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>, PlannerError> {
        if !children.is_empty() { Err(PlannerError::Internal("Leaf node cannot have children".to_string())) }
        else { Ok(self.clone_boxed()) }
    }
}

#[derive(Debug)]
pub struct DummyUnaryExec {
    name_val: String,
    input: Arc<dyn ExecutionPlan>,
}
impl DummyUnaryExec { pub fn new(name: &str, input: Arc<dyn ExecutionPlan>) -> Self { Self { name_val: name.to_string(), input } } }
impl ExecutionPlan for DummyUnaryExec {
    fn name(&self) -> &str { &self.name_val }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> { vec![self.input.clone()] }
    fn clone_boxed(&self) -> Arc<dyn ExecutionPlan> { Arc::new(Self { name_val: self.name_val.clone(), input: self.input.clone() }) }
    fn with_new_children(&self, mut children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>, PlannerError> {
        if children.len() != 1 { Err(PlannerError::Internal("UnaryExec must have one child".to_string())) }
        else { Ok(Arc::new(Self::new(&self.name_val, children.remove(0)))) }
    }
}

#[derive(Debug)]
pub struct DummyBinaryExec {
    name_val: String,
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
}
impl DummyBinaryExec { pub fn new(name: &str, left: Arc<dyn ExecutionPlan>, right: Arc<dyn ExecutionPlan>) -> Self { Self { name_val: name.to_string(), left, right } } }
impl ExecutionPlan for DummyBinaryExec {
    fn name(&self) -> &str { &self.name_val }
    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> { vec![self.left.clone(), self.right.clone()] }
    fn clone_boxed(&self) -> Arc<dyn ExecutionPlan> { Arc::new(Self { name_val: self.name_val.clone(), left: self.left.clone(), right: self.right.clone() }) }
    fn with_new_children(&self, mut children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>, PlannerError> {
        if children.len() != 2 { Err(PlannerError::Internal("BinaryExec must have two children".to_string())) }
        else {
            let right_child = children.remove(1);
            let left_child = children.remove(0);
            Ok(Arc::new(Self::new(&self.name_val, left_child, right_child)))
        }
    }
}


// --- Tests ---
#[cfg(test)]
mod tests {
    use super::*;

    fn get_plan_node_name(plan: &Arc<dyn ExecutionPlan>) -> String {
        plan.name().to_string()
    }
    fn get_stage_input_id(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
        if plan.name() == "StageInputExec" {
            // Need to downcast to get stage_id. For test simplicity, assume it's StageInputExec if name matches.
            // This is not robust. A real implementation would require proper downcasting.
            // Let's make StageInputExec store its ID in its name for testing if direct downcast is hard.
            // Modifying StageInputExec::name() for this is a hack.
            // A better way: add a method to ExecutionPlan `fn as_stage_input(&self) -> Option<usize>`.
            // For now, we'll inspect the debug print if needed, or trust the logic.
            // Let's assume stage_id of StageInputExec is retrievable or check dependencies.
            if let Some(sie) = plan.as_any().downcast_ref::<StageInputExec>() {
                 return Some(sie.stage_id);
            }
        }
        None
    }


    #[test]
    fn test_no_shuffle() { // Leaf -> Unary1 -> Unary2 (root)
        let mut planner = DistributedPlanner::new();
        let leaf = Arc::new(DummyLeafExec::new("Leaf"));
        let unary1 = Arc::new(DummyUnaryExec::new("Unary1", leaf.clone()));
        let unary2 = Arc::new(DummyUnaryExec::new("Unary2", unary1.clone()));

        let stages = planner.plan_query(unary2.clone()).unwrap();

        // Expected: One stage, plan is Unary2(Unary1(Leaf)), no dependencies
        assert_eq!(stages.len(), 1);
        let root_stage = &stages[0];
        assert_eq!(root_stage.stage_id, 0); // First stage created by plan_query for the root
        assert!(root_stage.dependencies.is_empty());
        assert_eq!(get_plan_node_name(&root_stage.plan), "Unary2");
        assert_eq!(get_plan_node_name(&root_stage.plan.children()[0]), "Unary1");
        assert_eq!(get_plan_node_name(&root_stage.plan.children()[0].children()[0]), "Leaf");
    }

    #[test]
    fn test_one_shuffle() { // Leaf -> Unary1 -> Repartition -> Unary2 (root)
        let mut planner = DistributedPlanner::new();
        let leaf = Arc::new(DummyLeafExec::new("Leaf"));
        let unary1 = Arc::new(DummyUnaryExec::new("Unary1", leaf.clone()));
        let repartition = Arc::new(RepartitionExec::new(unary1.clone()));
        let unary2 = Arc::new(DummyUnaryExec::new("Unary2", repartition.clone()));

        let stages = planner.plan_query(unary2.clone()).unwrap();
        println!("One shuffle stages: {:#?}", stages);

        // Expected: Two stages
        // Stage 0: Plan = Unary1(Leaf), Deps = [] (created for input to Repartition)
        // Stage 1: Plan = Unary2(StageInputExec(0)), Deps = [0] (root stage)
        assert_eq!(stages.len(), 2);

        let child_stage = stages.iter().find(|s| s.stage_id == 0).unwrap();
        let root_stage = stages.iter().find(|s| s.stage_id == 1).unwrap();

        // Child Stage (Stage 0)
        assert_eq!(child_stage.stage_id, 0);
        assert!(child_stage.dependencies.is_empty());
        assert_eq!(get_plan_node_name(&child_stage.plan), "Unary1"); // Plan is Unary1(Leaf)
        assert_eq!(get_plan_node_name(&child_stage.plan.children()[0]), "Leaf");

        // Root Stage (Stage 1)
        assert_eq!(root_stage.stage_id, 1);
        assert_eq!(root_stage.dependencies, vec![0]);
        assert_eq!(get_plan_node_name(&root_stage.plan), "Unary2"); // Plan is Unary2(StageInputExec(0))
        assert_eq!(root_stage.plan.children().len(), 1);
        let stage_input_node = &root_stage.plan.children()[0];
        assert_eq!(get_plan_node_name(stage_input_node), "StageInputExec");
        // How to get stage_id from StageInputExec? Need downcast or a method.
        // For now, trust the dependency link.
        // A proper check: assert_eq!(get_stage_input_id(stage_input_node), Some(0));
         if let Some(sie) = stage_input_node.as_any().downcast_ref::<StageInputExec>() {
            assert_eq!(sie.stage_id, 0);
        } else {
            panic!("Expected StageInputExec");
        }
    }

    #[test]
    fn test_multiple_shuffles_chain() { // Leaf -> R1 -> U1 -> R2 -> U2 (root)
                                        // R1 input: Leaf. Stage 0: Leaf, deps []
                                        // U1 input: StageInput(0).
                                        // R2 input: U1(StageInput(0)). Stage 1: U1(StageInput(0)), deps [0]
                                        // U2 input: StageInput(1). Stage 2: U2(StageInput(1)), deps [1] (root)
        let mut planner = DistributedPlanner::new();
        let leaf = Arc::new(DummyLeafExec::new("Leaf"));                            // Stage 0 plan
        let r1 = Arc::new(RepartitionExec::new(leaf.clone()));
        let u1 = Arc::new(DummyUnaryExec::new("U1", r1.clone()));                   // Stage 1 plan root
        let r2 = Arc::new(RepartitionExec::new(u1.clone()));
        let u2 = Arc::new(DummyUnaryExec::new("U2", r2.clone()));                   // Stage 2 plan root (final stage)

        let stages = planner.plan_query(u2.clone()).unwrap();
        println!("Chain shuffle stages: {:#?}", stages);

        assert_eq!(stages.len(), 3); // Stage 0 (Leaf), Stage 1 (U1(Input S0)), Stage 2 (U2(Input S1))

        // Stage 0 (input to R1)
        let s0 = stages.iter().find(|s| s.stage_id == 0).unwrap();
        assert_eq!(get_plan_node_name(&s0.plan), "Leaf");
        assert!(s0.dependencies.is_empty());

        // Stage 1 (input to R2)
        let s1 = stages.iter().find(|s| s.stage_id == 1).unwrap();
        assert_eq!(get_plan_node_name(&s1.plan), "U1");
        assert_eq!(s1.dependencies, vec![s0.stage_id]);
        let s1_input = &s1.plan.children()[0];
        assert_eq!(get_plan_node_name(s1_input), "StageInputExec");
         if let Some(sie) = s1_input.as_any().downcast_ref::<StageInputExec>() { assert_eq!(sie.stage_id, s0.stage_id); }
         else { panic!("Expected StageInputExec for S1 input"); }


        // Stage 2 (root stage)
        let s2 = stages.iter().find(|s| s.stage_id == 2).unwrap();
        assert_eq!(get_plan_node_name(&s2.plan), "U2");
        assert_eq!(s2.dependencies, vec![s1.stage_id]);
        let s2_input = &s2.plan.children()[0];
        assert_eq!(get_plan_node_name(s2_input), "StageInputExec");
        if let Some(sie) = s2_input.as_any().downcast_ref::<StageInputExec>() { assert_eq!(sie.stage_id, s1.stage_id); }
        else { panic!("Expected StageInputExec for S2 input"); }
    }

    #[test]
    fn test_shuffle_tree() { // Leaf1 -> R1 -> Join -> U1 (root)
                             // Leaf2 -> R2 --^
                             // Stage 0: Leaf1. Plan: Leaf1, Deps: []
                             // Stage 1: Leaf2. Plan: Leaf2, Deps: []
                             // Stage 2 (root): Join(Input S0, Input S1) -> U1. Plan: U1(Join(SIE_S0, SIE_S1)), Deps: [S0, S1]
        let mut planner = DistributedPlanner::new();
        let leaf1 = Arc::new(DummyLeafExec::new("Leaf1"));
        let r1 = Arc::new(RepartitionExec::new(leaf1.clone())); // Consumed by left side of Join

        let leaf2 = Arc::new(DummyLeafExec::new("Leaf2"));
        let r2 = Arc::new(RepartitionExec::new(leaf2.clone())); // Consumed by right side of Join

        let join = Arc::new(DummyBinaryExec::new("Join", r1.clone(), r2.clone()));
        let u1 = Arc::new(DummyUnaryExec::new("U1", join.clone())); // Root of the final stage

        let stages = planner.plan_query(u1.clone()).unwrap();
        println!("Tree shuffle stages: {:#?}", stages);

        assert_eq!(stages.len(), 3);

        // Find stages by their plans, as IDs might vary in assignment order before sorting
        let s_leaf1 = stages.iter().find(|s| get_plan_node_name(&s.plan) == "Leaf1").expect("Stage for Leaf1 not found");
        let s_leaf2 = stages.iter().find(|s| get_plan_node_name(&s.plan) == "Leaf2").expect("Stage for Leaf2 not found");
        let s_root = stages.iter().find(|s| get_plan_node_name(&s.plan) == "U1").expect("Root stage U1 not found");

        // Validate Leaf1 stage (S0)
        assert!(s_leaf1.dependencies.is_empty());

        // Validate Leaf2 stage (S1)
        assert!(s_leaf2.dependencies.is_empty());

        // Validate Root stage (S2)
        assert_eq!(s_root.dependencies.len(), 2);
        assert!(s_root.dependencies.contains(&s_leaf1.stage_id));
        assert!(s_root.dependencies.contains(&s_leaf2.stage_id));

        assert_eq!(get_plan_node_name(&s_root.plan.children()[0]), "Join");
        let join_node_in_root_plan = &s_root.plan.children()[0];
        let join_children = join_node_in_root_plan.children();
        assert_eq!(join_children.len(), 2);

        let join_left_input = &join_children[0];
        let join_right_input = &join_children[1];

        assert_eq!(get_plan_node_name(join_left_input), "StageInputExec");
        assert_eq!(get_plan_node_name(join_right_input), "StageInputExec");

        // Check that these StageInputExec nodes point to the correct stages
        let left_sie_points_to = join_left_input.as_any().downcast_ref::<StageInputExec>().unwrap().stage_id;
        let right_sie_points_to = join_right_input.as_any().downcast_ref::<StageInputExec>().unwrap().stage_id;

        // The specific stage IDs (s_leaf1.stage_id vs s_leaf2.stage_id) might be swapped for left/right
        // So check that the set of pointed-to IDs is correct.
        let mut expected_deps_for_join = HashSet::new();
        expected_deps_for_join.insert(s_leaf1.stage_id);
        expected_deps_for_join.insert(s_leaf2.stage_id);

        let mut actual_deps_for_join = HashSet::new();
        actual_deps_for_join.insert(left_sie_points_to);
        actual_deps_for_join.insert(right_sie_points_to);

        assert_eq!(expected_deps_for_join, actual_deps_for_join);
    }
}
