using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace NetworkSimplex
{
    public class FlowGraph
    {
        private readonly FlowNode[] _nodes;
        private readonly FlowArc[] _arcs;
        private readonly int[] _nodeArcs;

        public FlowGraph(IEnumerable<double> nodeSupplies, IEnumerable<FlowArc> arcs)
        {
            if (nodeSupplies == null)
                throw new ArgumentNullException(nameof(nodeSupplies));

            if (arcs == null)
                throw new ArgumentNullException(nameof(arcs));

            double[] nodeSupplyArr = nodeSupplies.ToArray();
            int numNodes = nodeSupplyArr.Length;

            if (Math.Abs(nodeSupplyArr.Sum(d => d)) > 0.00001)
                throw new ArgumentException("Balance constraint is not satisfied by nodes", nameof(nodeSupplies));

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
                nodes[i] = new FlowNode(i, nodeSupplyArr[i], count, inCounts[i], outCounts[i]);

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
            private readonly Stack<int> _stack = new Stack<int>();
            private readonly Queue<int> _queue = new Queue<int>();
            private int _tag;
            private int _seen;

            public FlowGraphSolver(FlowGraph graph)
            {
                _graph = graph;
                _nodeStates = new NodeState[graph._nodes.Length];
                _arcStates = new ArcState[graph._arcs.Length];
            }

            public FlowGraphSolution Solve()
            {
                _nodeStates[0].InTree = true;
                // Compute initial spanning tree, primal variables, dual variables and dual slack variables.
                // Always use first node as root.
                InitialVisit(0);

                if (_seen != _graph._nodes.Length)
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
                FlowNode[] nodes = _graph._nodes;
                int[] nodeArcs = _graph._nodeArcs;
                FlowArc[] arcs = _graph._arcs;
                NodeState[] nodeStates = _nodeStates;
                ArcState[] arcStates = _arcStates;

                FlowNode node = nodes[root];
                ref NodeState nodeState = ref nodeStates[root];
                nodeState.Supply = node.Supply;
                _seen++;

                for (int i = node.ArcsIndex, j = node.ArcsIndex + node.CountIn + node.CountOut; i < j; i++)
                {
                    int arcIndex = nodeArcs[i];
                    FlowArc arc = arcs[arcIndex];
                    ref ArcState arcState = ref arcStates[arcIndex];

                    int nei = arc.Source == root ? arc.Target : arc.Source;
                    ref NodeState neiState = ref nodeStates[nei];
                    if (neiState.InTree)
                    {
                        // Non-tree edge. Compute slack
                        // slackIJ = dualI + costIJ - dualJ
                        if (arc.Source == root)
                            arcState.Value = nodeState.DualValue + arc.Cost - neiState.DualValue;
                        else
                            arcState.Value = neiState.DualValue + arc.Cost - nodeState.DualValue;

                        continue;
                    }

                    neiState.InTree = true;
                    arcState.IsTree = true;

                    // Dual feasibility condition states, for edge (i, j):
                    // dualJ - dualI = costIJ
                    if (arc.Source == root)
                        neiState.DualValue = arc.Cost + nodeState.DualValue;
                    else
                        neiState.DualValue = nodeState.DualValue - arc.Cost;

                    InitialVisit(nei);

                    // If outgoing then we must send negative amount of flow to make sum 0
                    // If incoming then we can just send flow along the edge to make it 0
                    if (arc.Source == root)
                        arcState.Value = -neiState.Supply;
                    else
                        arcState.Value = neiState.Supply;

                    // In any case we move the supply from neighbor to root
                    nodeState.Supply += neiState.Supply;
                }
            }

            private FlowGraphSolution PivotStep()
            {
                FlowArc[] arcs = _graph._arcs;
                ArcState[] arcStates = _arcStates;

                for (int i = 0; i < arcs.Length; i++)
                {
                    if (arcStates[i].IsTree || arcStates[i].Value >= 0)
                        continue;

                    if (!PrimalPivot(i))
                        return new FlowGraphSolution(SolutionType.Unbounded, Array.Empty<double>());

                    return null;
                }

                for (int i = 0; i < arcs.Length; i++)
                {
                    if (!arcStates[i].IsTree || arcStates[i].Value >= 0)
                        continue;

                    if (!DualPivot(i))
                        return new FlowGraphSolution(SolutionType.Infeasible, Array.Empty<double>());

                    return null;
                }

                return new FlowGraphSolution(SolutionType.Feasible, arcStates.Select(a => a.IsTree ? a.Value : 0).ToArray());
            }

            /// <summary>
            /// Performs a primal pivot with the specified entering arc.
            /// </summary>
            private bool PrimalPivot(int enteringArcIndex)
            {
                FlowArc[] arcs = _graph._arcs;
                NodeState[] nodeStates = _nodeStates;
                ArcState[] arcStates = _arcStates;

                FlowArc enteringArc = arcs[enteringArcIndex];
                ref ArcState enteringArcState = ref arcStates[enteringArcIndex];

                FindCycleOnArcAddition(enteringArcIndex);
                // Find smallest flow in cycle that runs in the opposite direction of entering arc.
                // This could be done directly in the loop above as CPU/memory trade-off.
                double flow = double.MaxValue;
                int leavingArcIndex = -1;
                int cur = enteringArc.Source;
                do
                {
                    int parArcIndex = nodeStates[cur].ParentCycleIndex;
                    FlowArc parArc = arcs[parArcIndex];
                    ref ArcState parArcState = ref arcStates[parArcIndex];
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

                if (leavingArcIndex == -1)
                    return false;

                bool updateSource = TagSubTree(leavingArcIndex);

                UpdateTreeForCycle(enteringArcIndex, leavingArcIndex, flow, updateSource);
                return true;
            }

            /// <summary>
            /// Finds the cycle that would result when adding the specified arc to the current tree.
            /// The cycle is stored in the node states through parent arc indices.
            /// </summary>
            private void FindCycleOnArcAddition(int arcIndex)
            {
                FlowNode[] nodes = _graph._nodes;
                int[] nodeArcs = _graph._nodeArcs;
                FlowArc[] arcs = _graph._arcs;
                NodeState[] nodeStates = _nodeStates;
                ArcState[] arcStates = _arcStates;
                Queue<int> queue = _queue;

                FlowArc enteringArc = arcs[arcIndex];
                ref ArcState enteringArcState = ref arcStates[arcIndex];

                int tag = ++_tag;
                // BFS is best in practice.
                queue.Clear();
                queue.Enqueue(enteringArc.Target);
                nodeStates[enteringArc.Target].ParentCycleIndex = arcIndex;
                nodeStates[enteringArc.Target].Tag = tag;

                // Find cycle when arcIndex enters.
                while (true)
                {
                    int nodeIndex = queue.Dequeue();
                    FlowNode node = nodes[nodeIndex];
                    ref NodeState nodeState = ref nodeStates[nodeIndex];

                    for (int i = node.ArcsIndex, j = node.ArcsIndex + node.CountIn + node.CountOut; i < j; i++)
                    {
                        int neiArcIndex = nodeArcs[i];
                        // Only follow tree edges
                        if (!arcStates[neiArcIndex].IsTree)
                            continue;

                        FlowArc neiArc = arcs[neiArcIndex];
                        int neiNodeIndex = neiArc.Source == nodeIndex ? neiArc.Target : neiArc.Source;
                        ref NodeState neiNodeState = ref nodeStates[neiNodeIndex];
                        if (neiNodeState.Tag == tag)
                            continue;

                        neiNodeState.ParentCycleIndex = neiArcIndex;
                        // When we find the source of the arc we are pivoting on we have found the full cycle.
                        // Get out of the loop ASAP.
                        if (neiNodeIndex == enteringArc.Source)
                            return;

                        neiNodeState.Tag = tag;
                        queue.Enqueue(neiNodeIndex);
                    }
                }
            }

            /// <summary>
            /// Simulates removing the specified arc and then tags the sub-tree containing
            /// the source node. Returns true if the smallest sub-tree is the one containing
            /// the source node.
            /// </summary>
            private bool TagSubTree(int leavingArcIndex)
            {
                FlowNode[] nodes = _graph._nodes;
                int[] nodeArcs = _graph._nodeArcs;
                FlowArc[] arcs = _graph._arcs;
                NodeState[] nodeStates = _nodeStates;
                ArcState[] arcStates = _arcStates;
                Stack<int> stack = _stack;

                FlowArc leavingArc = arcs[leavingArcIndex];
                ref ArcState leavingArcState = ref arcStates[leavingArcIndex];

                leavingArcState.IsTree = false;

                int tag = ++_tag;
                int count = 0;

                stack.Clear();
                stack.Push(leavingArc.Source);
                nodeStates[leavingArc.Source].SubTreeTag = tag;

                while (stack.Count > 0)
                {
                    count++;

                    int nodeIndex = stack.Pop();
                    FlowNode node = nodes[nodeIndex];
                    ref NodeState nodeState = ref nodeStates[nodeIndex];

                    for (int i = node.ArcsIndex, j = node.ArcsIndex + node.CountIn + node.CountOut; i < j; i++)
                    {
                        int neiArcIndex = nodeArcs[i];
                        FlowArc neiArc = arcs[neiArcIndex];
                        ref ArcState neiArcState = ref arcStates[neiArcIndex];

                        if (!neiArcState.IsTree)
                            continue;

                        int neiNodeIndex = neiArc.Source == nodeIndex ? neiArc.Target : neiArc.Source;
                        FlowNode neiNode = nodes[neiNodeIndex];
                        ref NodeState neiState = ref nodeStates[neiNodeIndex];

                        if (neiState.SubTreeTag == tag)
                            continue;

                        neiState.SubTreeTag = tag;
                        stack.Push(neiNodeIndex);
                    }
                }

                leavingArcState.IsTree = true;
                return count * 2 <= nodes.Length;
            }

            /// <summary>
            /// Updates the current tree by respectively adding and removing the specified arcs.
            /// This method expects the cycle containing the arcs to be stored in the node states.
            /// It also expects the sub-tree containing the source node of the leaving arc to be tagged.
            /// The specified flow is pushed along the cycle before the arcs are added and removed.
            /// updateSource indicates whether the method should use the source or target sub-tree of the leaving
            /// arc when updating dual slack variables.
            /// </summary>
            private bool UpdateTreeForCycle(int enteringArcIndex, int leavingArcIndex, double flow, bool updateSource)
            {
                FlowNode[] nodes = _graph._nodes;
                int[] nodeArcs = _graph._nodeArcs;
                FlowArc[] arcs = _graph._arcs;
                NodeState[] nodeStates = _nodeStates;
                ArcState[] arcStates = _arcStates;
                Stack<int> stack = _stack;

                FlowArc enteringArc = arcs[enteringArcIndex];
                ref ArcState enteringArcState = ref arcStates[enteringArcIndex];
                FlowArc leavingArc = arcs[leavingArcIndex];
                ref ArcState leavingArcState = ref arcStates[leavingArcIndex];

                // Update primal flows.
                int cur = enteringArc.Source;
                do
                {
                    int parArcIndex = nodeStates[cur].ParentCycleIndex;
                    FlowArc parArc = arcs[parArcIndex];
                    ref ArcState parArcState = ref arcStates[parArcIndex];
                    double old = parArcState.Value;
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

                // Disconnect the sub trees.
                leavingArcState.IsTree = false;
                leavingArcState.Value = 0;

                // Update dual slacks for crossing non-tree edges
                int tag = ++_tag;

                int subTreeTag = nodeStates[leavingArc.Source].SubTreeTag;
                // "The dual slacks corresponding to those arcs that bridge in the same direction
                //  as the entering arc get decremented by the old dual slack on the entering arc,
                //  whereas those that correspond to arcs bridging in the opposite direction get
                //  incremented by this amount."
                // So if we are updating source and entering arc starts in source, we must flip.
                // Or if we update target and entering arc starts in target.
                double startInSubTreeIncrease = enteringArcState.Value;
                if (updateSource == (nodeStates[enteringArc.Source].SubTreeTag == subTreeTag))
                    startInSubTreeIncrease *= -1;

                stack.Clear();
                stack.Push(updateSource ? leavingArc.Source : leavingArc.Target);
                nodeStates[stack.Peek()].Tag = tag;

                while (stack.Count > 0)
                {
                    int nodeIndex = stack.Pop();
                    FlowNode node = nodes[nodeIndex];
                    ref NodeState nodeState = ref nodeStates[nodeIndex];

                    for (int i = node.ArcsIndex, j = node.ArcsIndex + node.CountIn + node.CountOut; i < j; i++)
                    {
                        int arcIndex = nodeArcs[i];
                        FlowArc arc = arcs[arcIndex];
                        ref ArcState arcState = ref arcStates[arcIndex];

                        int neiNodeIndex = arc.Source == nodeIndex ? arc.Target : arc.Source;
                        ref NodeState neiState = ref nodeStates[neiNodeIndex];

                        if (!arcState.IsTree)
                        {
                            // Update dual slack if this non-tree edge crosses between the sub-trees
                            if ((nodeState.SubTreeTag == subTreeTag) != (neiState.SubTreeTag == subTreeTag))
                                arcState.Value += startInSubTreeIncrease * (arc.Source == nodeIndex ? 1 : -1);

                            continue;
                        }

                        if (neiState.Tag == tag)
                            continue;

                        neiState.Tag = tag;
                        stack.Push(neiNodeIndex);
                    }
                }

                // Reconnect sub trees with new tree edge.
                enteringArcState.IsTree = true;
                enteringArcState.Value = flow;

                return true;
            }

            /// <summary>
            /// Performs a dual pivot with the specified leaving arc.
            /// </summary>
            private bool DualPivot(int leavingArcIndex)
            {
                FlowNode[] nodes = _graph._nodes;
                int[] nodeArcs = _graph._nodeArcs;
                FlowArc[] arcs = _graph._arcs;
                NodeState[] nodeStates = _nodeStates;
                ArcState[] arcStates = _arcStates;
                Stack<int> stack = _stack;

                FlowArc leavingArc = arcs[leavingArcIndex];
                ref ArcState leavingArcState = ref arcStates[leavingArcIndex];

                bool sourceIsSmallest = TagSubTree(leavingArcIndex);

                // Find entering arc index. It must bridge the subtrees in opposite direction
                // and have the smallest dual slack.
                leavingArcState.IsTree = false;
                int tag = ++_tag;
                int subTreeTag = nodeStates[leavingArc.Source].SubTreeTag;

                stack.Clear();
                stack.Push(sourceIsSmallest ? leavingArc.Source : leavingArc.Target);
                nodeStates[stack.Peek()].Tag = tag;

                int enteringArcIndex = -1;
                double minDualSlack = double.MaxValue;
                while (stack.Count > 0)
                {
                    int nodeIndex = stack.Pop();
                    FlowNode node = nodes[nodeIndex];
                    ref NodeState nodeState = ref nodeStates[nodeIndex];

                    for (int i = node.ArcsIndex, j = node.ArcsIndex + node.CountIn + node.CountOut; i < j; i++)
                    {
                        int arcIndex = nodeArcs[i];
                        FlowArc arc = arcs[arcIndex];
                        ref ArcState arcState = ref arcStates[arcIndex];

                        int neiNodeIndex = arc.Source == nodeIndex ? arc.Target : arc.Source;
                        ref NodeState neiState = ref nodeStates[neiNodeIndex];

                        if (!arcState.IsTree)
                        {
                            // Update entering arc if this non-tree edge crosses between the sub-trees
                            // in opposite order of leaving arc
                            bool startsInSmallest = arc.Source == nodeIndex;
                            if ((nodeState.SubTreeTag == subTreeTag) != (neiState.SubTreeTag == subTreeTag) &&
                                sourceIsSmallest != startsInSmallest)
                            {
                                if (arcState.Value < minDualSlack)
                                {
                                    minDualSlack = arcState.Value;
                                    enteringArcIndex = arcIndex;
                                }
                                else if (arcState.Value == minDualSlack)
                                    enteringArcIndex = Math.Min(enteringArcIndex, arcIndex);
                            }

                            continue;
                        }

                        if (neiState.Tag == tag)
                            continue;

                        neiState.Tag = tag;
                        stack.Push(neiNodeIndex);
                    }
                }

                leavingArcState.IsTree = true;

                if (enteringArcIndex == -1)
                    return false;

                FindCycleOnArcAddition(enteringArcIndex);
                UpdateTreeForCycle(enteringArcIndex, leavingArcIndex, -leavingArcState.Value, sourceIsSmallest);
                return true;
            }

            [StructLayout(LayoutKind.Auto)]
            private struct NodeState
            {
                public bool InTree { get; set; }
                public double Supply { get; set; }
                public double DualValue { get; set; }
                public int Tag { get; set; }
                public int SubTreeTag { get; set; }
                public int ParentCycleIndex { get; set; }
            }

            [StructLayout(LayoutKind.Auto)]
            private struct ArcState
            {
                public bool IsTree { get; set; }
                public double Value { get; set; }
            }
        }
    }

    public class FlowNode
    {
        internal FlowNode(int index, double suppply, int arcsIndex, int countIn, int countOut)
        {
            Index = index;
            Supply = suppply;
            ArcsIndex = arcsIndex;
            CountIn = countIn;
            CountOut = countOut;
        }

        public int Index { get; }
        public double Supply { get; }
        internal int ArcsIndex { get; }
        internal int CountIn { get; }
        internal int CountOut { get; }

        public override string ToString() => FormattableString.Invariant($"#{Index} Supply {Supply}");
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
