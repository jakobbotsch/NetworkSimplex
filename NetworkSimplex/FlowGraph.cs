using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace NetworkSimplex
{
    public class FlowGraph
    {
        private readonly FlowNode[] _nodes;
        private readonly FlowArc[] _arcs;
        private readonly int[] _nodeArcs;

        public FlowGraph(IEnumerable<double> nodeBalances, IEnumerable<FlowArc> arcs)
        {
            if (nodeBalances == null)
                throw new ArgumentNullException(nameof(nodeBalances));

            if (arcs == null)
                throw new ArgumentNullException(nameof(arcs));

            double[] nodeBalancesArr = nodeBalances.ToArray();
            int numNodes = nodeBalancesArr.Length;

            if (Math.Abs(nodeBalancesArr.Sum(d => d)) > 0.00001)
                throw new ArgumentException("Balance constraint is not satisfied by nodes", nameof(nodeBalances));

            FlowArc[] arcsArr = arcs.ToArray();
            foreach (FlowArc arc in arcsArr)
            {
                if (arc.Source < 0 || arc.Source >= numNodes)
                    throw new ArgumentException("An arc source is out of range", nameof(arcs));

                if (arc.Target < 0 || arc.Target >= numNodes)
                    throw new ArgumentException("An arc target is out of range", nameof(arcs));

                if (arc.Source == arc.Target)
                    throw new ArgumentException("All arcs must be between two distinct nodes", nameof(arcs));
            }

            int[] inCounts = new int[numNodes];
            int[] outCounts = new int[numNodes];
            foreach (FlowArc arc in arcsArr)
            {
                inCounts[arc.Target]++;
                outCounts[arc.Source]++;
            }

            FlowNode[] nodes = new FlowNode[numNodes];
            int[] writeIndices = new int[numNodes * 2];
            int count = 0;
            for (int i = 0; i < numNodes; i++)
            {
                nodes[i] = new FlowNode(i, nodeBalancesArr[i], count, inCounts[i], outCounts[i]);

                writeIndices[i * 2 + 0] = count;
                count += inCounts[i];
                writeIndices[i * 2 + 1] = count;
                count += outCounts[i];
            }

            int[] nodeArcs = new int[count];
            for (int i = 0; i < arcsArr.Length; i++)
            {
                FlowArc arc = arcsArr[i];
                ref int inIndex = ref writeIndices[arc.Target * 2 + 0];
                ref int outIndex = ref writeIndices[arc.Source * 2 + 1];

                nodeArcs[inIndex++] = i;
                nodeArcs[outIndex++] = i;
            }

            _nodes = nodes;
            _arcs = arcsArr;
            _nodeArcs = nodeArcs;
        }

        public IReadOnlyList<FlowNode> Nodes => _nodes;
        public IReadOnlyList<FlowArc> Arcs => _arcs;

        public IEnumerable<int> GetIncomingArcs(FlowNode node)
        {
            if (node == null)
                throw new ArgumentNullException(nameof(node));

            for (int i = node.ArcsIndex, j = 0; j < node.CountIn; i++, j++)
                yield return _nodeArcs[i];
        }

        public IEnumerable<int> GetOutgoingArcs(FlowNode node)
        {
            if (node == null)
                throw new ArgumentNullException(nameof(node));

            for (int i = node.ArcsIndex + node.CountIn, j = 0; j < node.CountOut; i++, j++)
                yield return _nodeArcs[i];
        }

        public FlowGraphSolution Solve()
        {
            if (_nodes.Length <= 0)
                return new FlowGraphSolution(SolutionType.Feasible, Array.Empty<double>());

            return new FlowGraphSolver(this).Solve();
        }

        public override string ToString() => $"{_nodes.Length} nodes, {_arcs.Length} arcs";

        private class FlowGraphSolver
        {
            private readonly FlowGraph _graph;
            private readonly NodeState[] _nodeStates;
            private readonly ArcState[] _arcStates;
            private readonly Stack<int> stack = new Stack<int>();
            private int _tag = 0;
            private int _seen = 0;

            public FlowGraphSolver(FlowGraph graph)
            {
                _graph = graph;
                _nodeStates = new NodeState[graph._nodes.Length];
                _arcStates = new ArcState[graph._arcs.Length];
            }

            public FlowGraphSolution Solve()
            {
                _seen++;
                _nodeStates[0].InTree = true;
                // Compute initial spanning tree, primal variables, dual variables and dual slack variables.
                // Always use first node as root.
                InitialVisit(0);

                if (_seen < _graph._nodes.Length)
                    throw new InvalidOperationException("Flow graph is not connected");

                while (true)
                {
                    FlowGraphSolution result = PivotStep();
                    if (result != null)
                        return result;
                }
            }

            /// <summary>
            /// Runs DFS to find an initial spanning tree while simultaneously assigning
            /// primal variables, dual variables and dual slack variables.
            /// </summary>
            private void InitialVisit(int root)
            {
                FlowNode node = _graph._nodes[root];
                ref NodeState nodeState = ref _nodeStates[root];
                nodeState.Supply = node.Balance;

                for (int i = node.ArcsIndex, j = node.ArcsIndex + node.CountIn + node.CountOut; i < j; i++)
                {
                    int arcId = _graph._nodeArcs[i];
                    FlowArc arc = _graph._arcs[arcId];
                    ref ArcState arcState = ref _arcStates[arcId];

                    int nei = arc.Source == root ? arc.Target : arc.Source;
                    ref NodeState neiState = ref _nodeStates[nei];
                    if (neiState.InTree)
                    {
                        // Non-tree edge. Compute slack
                        // slackIJ = dualI + costIJ - dualJ
                        if (arc.Source == root)
                            arcState.Value = nodeState.DualValue + arc.Cost - neiState.DualValue;
                        else
                            arcState.Value = neiState.DualValue + arc.Cost - nodeState.DualValue;

                        Console.WriteLine("{0} -> {1} is assigned slack {2}", (char)('a' + arc.Source), (char)('a' + arc.Target), arcState.Value);
                        continue;
                    }

                    _seen++;
                    neiState.InTree = true;
                    arcState.IsTree = true;

                    // Dual feasibility condition states, for edge (i, j):
                    // dualJ - dualI = costIJ
                    if (arc.Source == root)
                        neiState.DualValue = arc.Cost + nodeState.DualValue;
                    else
                        neiState.DualValue = nodeState.DualValue - arc.Cost;

                    Console.WriteLine("{0} gets dual value {1}", (char)('a' + nei), neiState.DualValue);

                    InitialVisit(nei);

                    // If outgoing then we must send negative amount of flow to make sum 0
                    // If incoming then we can just send flow along the edge to make it 0
                    if (arc.Source == root)
                        arcState.Value = -neiState.Supply;
                    else
                        arcState.Value = neiState.Supply;

                    // In any case we move the supply from neighbor to root
                    nodeState.Supply += neiState.Supply;
                    Console.WriteLine("{0} -> {1} is tree edge with {2} flow", (char)('a' + arc.Source), (char)('a' + arc.Target), arcState.Value);
                }
            }

            private FlowGraphSolution PivotStep()
            {
                for (int i = 0; i < _graph._arcs.Length; i++)
                {
                    if (_arcStates[i].IsTree || _arcStates[i].Value >= 0)
                        continue;

                    if (!PrimalPivot(i))
                        return new FlowGraphSolution(SolutionType.Unbounded, Array.Empty<double>());

                    return null;
                }

                for (int i = 0; i < _graph._arcs.Length; i++)
                {
                    if (!_arcStates[i].IsTree || _arcStates[i].Value >= 0)
                        continue;

                    if (!DualPivot(i))
                        return new FlowGraphSolution(SolutionType.Infeasible, Array.Empty<double>());

                    return null;
                }

                return new FlowGraphSolution(SolutionType.Feasible, _arcStates.Select(a => a.IsTree ? a.Value : 0).ToArray());
            }

            /// <summary>
            /// Performs a primal pivot with the specified entering arc.
            /// </summary>
            private bool PrimalPivot(int enteringArcIndex)
            {
                FlowArc enteringArc = _graph._arcs[enteringArcIndex];
                ref ArcState enteringArcState = ref _arcStates[enteringArcIndex];
                Console.WriteLine("Entering (primal privot) {0} -> {1}", (char)('a' + enteringArc.Source), (char)('a' + enteringArc.Target));

                FindCycleOnArcAddition(enteringArcIndex);
                Console.Write("Cycle is:");
                // Find smallest flow in cycle that runs in the opposite direction of entering arc.
                // This could be done directly in the loop above as CPU/memory trade-off.
                double flow = double.MaxValue;
                int leavingArcIndex = -1;
                int cur = enteringArc.Source;
                do
                {
                    Console.Write(" {0}", (char)('a' + cur));
                    int parArcIndex = _nodeStates[cur].ParentArcOrTag;
                    FlowArc parArc = _graph._arcs[parArcIndex];
                    ref ArcState parArcState = ref _arcStates[parArcIndex];
                    if (parArc.Source == cur)
                    {
                        // This is the opposite direction of entering arc.
                        if (parArcState.Value < flow)
                        {
                            flow = parArcState.Value;
                            leavingArcIndex = parArcIndex;
                        }
                        else if (parArcState.Value == flow)
                            leavingArcIndex = Math.Min(parArcIndex, leavingArcIndex); // Bland's rule

                        cur = parArc.Target;
                    }
                    else
                        cur = parArc.Source;

                } while (cur != enteringArc.Target);

                Console.WriteLine(" {0}", (char)('a' + cur));

                if (leavingArcIndex == -1)
                {
                    // Unbounded
                    return false;
                }

                Console.WriteLine("Leaving arc is {0} -> {1}. Sending {2} flow around", (char)('a' + _graph._arcs[leavingArcIndex].Source), (char)('a' + _graph._arcs[leavingArcIndex].Target), flow);
                UpdateTreeForCycle(enteringArcIndex, leavingArcIndex, flow);
                return true;
            }

            /// <summary>
            /// Finds the cycle that would result when adding the specified arc to the current tree.
            /// The cycle is stored in the node states through parent arc indices.
            /// </summary>
            private void FindCycleOnArcAddition(int arcIndex)
            {
                FlowArc enteringArc = _graph._arcs[arcIndex];
                ref ArcState enteringArcState = ref _arcStates[arcIndex];

                stack.Clear();
                stack.Push(enteringArc.Target);
                _nodeStates[enteringArc.Target].ParentArcOrTag = arcIndex;

                // Find cycle when arcIndex enters.
                while (true)
                {
                    int nodeIndex = stack.Pop();
                    FlowNode node = _graph._nodes[nodeIndex];
                    ref NodeState nodeState = ref _nodeStates[nodeIndex];

                    for (int i = node.ArcsIndex, j = node.ArcsIndex + node.CountIn + node.CountOut; i < j; i++)
                    {
                        int neiArcIndex = _graph._nodeArcs[i];
                        // Only follow tree edges and do not expand backwards.
                        if (!_arcStates[neiArcIndex].IsTree || nodeState.ParentArcOrTag == neiArcIndex)
                            continue;

                        FlowArc neiArc = _graph._arcs[neiArcIndex];
                        int neiNodeIndex = neiArc.Source == nodeIndex ? neiArc.Target : neiArc.Source;
                        _nodeStates[neiNodeIndex].ParentArcOrTag = neiArcIndex;
                        // When we find the source of the arc we are pivoting on we have found the full cycle.
                        // Get out of the loop ASAP.
                        if (neiNodeIndex == enteringArc.Source)
                            return;

                        stack.Push(neiNodeIndex);
                    }
                }
            }

            /// <summary>
            /// Updates the current tree by respectively removing and adding the specified arcs.
            /// This method expects the cycle containing the arcs to be stored in the node states.
            /// The specified flow is pushed along the cycle before the arcs or added and removed.
            /// </summary>
            private bool UpdateTreeForCycle(int enteringArcIndex, int leavingArcIndex, double flow)
            {
                FlowArc enteringArc = _graph._arcs[enteringArcIndex];
                ref ArcState enteringArcState = ref _arcStates[enteringArcIndex];

                // Send flow around in cycle.
                int cur = enteringArc.Source;
                do
                {
                    int parArcIndex = _nodeStates[cur].ParentArcOrTag;
                    FlowArc parArc = _graph._arcs[parArcIndex];
                    ref ArcState parArcState = ref _arcStates[parArcIndex];
                    if (parArc.Source == cur)
                    {
                        parArcState.Value -= flow;
                        cur = parArc.Target;
                    }
                    else
                    {
                        parArcState.Value += flow;
                        cur = parArc.Source;
                    }
                } while (cur != enteringArc.Target);

                // Enter pivoting arc and leave the lowest one.
                enteringArcState.IsTree = true;
                enteringArcState.Value = flow;
                _arcStates[leavingArcIndex].IsTree = false;

                // Recompute dual and slack variables
                AssignDualVariables();
                AssignDualSlacks();
                return true;
            }

            /// <summary>
            /// Runs DFS to assign dual variables with the 0th variable as the root.
            /// </summary>
            private void AssignDualVariables()
            {
                _tag--;
                _nodeStates[0].DualValue = 0;
                _nodeStates[0].ParentArcOrTag = _tag;
                AssignDualVariablesVisit(0);
            }

            private void AssignDualVariablesVisit(int nodeIndex)
            {
                FlowNode node = _graph._nodes[nodeIndex];
                ref NodeState nodeState = ref _nodeStates[nodeIndex];

                for (int i = node.ArcsIndex, j = node.ArcsIndex + node.CountIn + node.CountOut; i < j; i++)
                {
                    int neiArcIndex = _graph._nodeArcs[i];
                    FlowArc neiArc = _graph._arcs[neiArcIndex];
                    ref ArcState neiArcState = ref _arcStates[neiArcIndex];

                    if (!neiArcState.IsTree)
                        continue;

                    int neiIndex = neiArc.Source == nodeIndex ? neiArc.Target : neiArc.Source;
                    FlowNode neiNode = _graph._nodes[neiIndex];
                    ref NodeState neiState = ref _nodeStates[neiIndex];

                    if (neiState.ParentArcOrTag == _tag)
                        continue;

                    neiState.ParentArcOrTag = _tag;

                    // Dual feasibility condition states, for edge (i, j):
                    // dualJ - dualI = costIJ
                    if (neiArc.Source == nodeIndex)
                        neiState.DualValue = neiArc.Cost + nodeState.DualValue;
                    else
                        neiState.DualValue = nodeState.DualValue - neiArc.Cost;

                    Console.WriteLine("{0} gets dual value {1}", (char)('a' + neiIndex), neiState.DualValue);
                    AssignDualVariablesVisit(neiIndex);
                }
            }

            /// <summary>
            /// Assigns dual slack to all non-tree arcs.
            /// </summary>
            private void AssignDualSlacks()
            {
                for (int arcId = 0; arcId < _graph._arcs.Length; arcId++)
                {
                    FlowArc arc = _graph._arcs[arcId];
                    ref ArcState arcState = ref _arcStates[arcId];
                    if (arcState.IsTree)
                        continue;

                    ref NodeState nodeI = ref _nodeStates[arc.Source];
                    ref NodeState nodeJ = ref _nodeStates[arc.Target];
                    // Non-tree edge. Compute slack
                    // slackIJ = dualI + costIJ - dualJ
                    arcState.Value = nodeI.DualValue + arc.Cost - nodeJ.DualValue;

                    Console.WriteLine("{0} -> {1} is assigned slack {2}", (char)('a' + arc.Source), (char)('a' + arc.Target), arcState.Value);
                }
            }

            /// <summary>
            /// Performs a dual pivot with the specified leaving arc.
            /// </summary>
            private bool DualPivot(int leavingArcIndex)
            {
                FlowArc leavingArc = _graph._arcs[leavingArcIndex];
                Console.WriteLine("Leaving (dual pivot) {0} -> {1}", (char)('a' + leavingArc.Source), (char)('a' + leavingArc.Target));
                ref ArcState leavingArcState = ref _arcStates[leavingArcIndex];

                leavingArcState.IsTree = false;
                // Tag part of tree that still contains root
                _tag--;
                stack.Clear();
                stack.Push(0);
                _nodeStates[0].ParentArcOrTag = _tag;

                while (stack.Count > 0)
                {
                    int nodeIndex = stack.Pop();
                    FlowNode node = _graph._nodes[nodeIndex];
                    ref NodeState nodeState = ref _nodeStates[nodeIndex];

                    for (int i = node.ArcsIndex, j = node.ArcsIndex + node.CountIn + node.CountOut; i < j; i++)
                    {
                        int neiArcIndex = _graph._nodeArcs[i];
                        FlowArc neiArc = _graph._arcs[neiArcIndex];
                        ref ArcState neiArcState = ref _arcStates[neiArcIndex];
                        // Only follow tree edges
                        if (!_arcStates[neiArcIndex].IsTree)
                            continue;

                        int neiIndex = neiArc.Source == nodeIndex ? neiArc.Target : neiArc.Source;
                        FlowNode nei = _graph._nodes[neiIndex];
                        ref NodeState neiState = ref _nodeStates[neiIndex];
                        // Do not expand backwards
                        if (neiState.ParentArcOrTag == _tag)
                            continue;

                        neiState.ParentArcOrTag = _tag;
                        stack.Push(neiIndex);
                    }
                }

                // Find entering arc index. It must bridge the subtrees in opposite direction
                // and have the smallest dual slack.
                bool leavingStartsInRoot = _nodeStates[leavingArc.Source].ParentArcOrTag == _tag;

                int enteringArcIndex = -1;
                double minDualSlack = double.MaxValue;
                for (int i = 0; i < _graph._arcs.Length; i++)
                {
                    FlowArc arc = _graph._arcs[i];
                    ref ArcState arcState = ref _arcStates[i];
                    if (arcState.IsTree)
                        continue;

                    ref NodeState sourceNodeState = ref _nodeStates[arc.Source];
                    ref NodeState targetNodeState = ref _nodeStates[arc.Target];
                    bool startsInRoot = _tag == sourceNodeState.ParentArcOrTag;
                    bool endsInRoot = _tag == targetNodeState.ParentArcOrTag;

                    // If we do not cross between the subtrees, then this is not a feasible entering arc.
                    if (startsInRoot == endsInRoot)
                        continue;

                    // Ok, this crosses. Ensure the bridge direction is the opposite of the leaving arc.
                    if (startsInRoot == leavingStartsInRoot)
                        continue;

                    // Ok, this bridges in the opposite direction.
                    if (arcState.Value < minDualSlack)
                    {
                        minDualSlack = arcState.Value;
                        enteringArcIndex = i;
                    }
                }

                if (enteringArcIndex == -1)
                    return false;

                leavingArcState.IsTree = true;
                FindCycleOnArcAddition(enteringArcIndex);
                UpdateTreeForCycle(enteringArcIndex, leavingArcIndex, -leavingArcState.Value);
                return true;
            }

            private struct NodeState
            {
                public bool InTree { get; set; }
                public double Supply { get; set; }
                public double DualValue { get; set; }
                public int ParentArcOrTag { get; set; }
            }

            private struct ArcState
            {
                public bool IsTree { get; set; }
                public double Value { get; set; }
            }
        }
    }

    public class FlowNode
    {
        internal FlowNode(int index, double balance, int arcsIndex, int countIn, int countOut)
        {
            Index = index;
            Balance = balance;
            ArcsIndex = arcsIndex;
            CountIn = countIn;
            CountOut = countOut;
        }

        public int Index { get; }
        public double Balance { get; }
        internal int ArcsIndex { get; }
        internal int CountIn { get; }
        internal int CountOut { get; }

        public override string ToString() => FormattableString.Invariant($"#{Index} Balance {Balance}");
    }

    public class FlowArc
    {
        public FlowArc(int source, int target, double cost)
        {
            Source = source;
            Target = target;
            Cost = cost;
        }

        public int Source { get; }
        public int Target { get; }
        public double Cost { get; }

        public override string ToString() => FormattableString.Invariant($"#{Source} -> #{Target} for {Cost}");
    }
}
