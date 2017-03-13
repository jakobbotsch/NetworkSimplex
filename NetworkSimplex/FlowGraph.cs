using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            private int _rootNode = 'g' - 'a';
            private readonly FlowGraph _graph;
            private readonly NodeState[] _nodeStates;
            private readonly ArcState[] _arcStates;
            private readonly Stack<int> _stack = new Stack<int>();
            private readonly List<int> _cycle = new List<int>();
            private int _tag;

            public FlowGraphSolver(FlowGraph graph)
            {
                _graph = graph;
                _nodeStates = new NodeState[graph._nodes.Length];
                _arcStates = new ArcState[graph._arcs.Length];
            }

            public FlowGraphSolution Solve()
            {
                int seen = ComputeInitialValues();

                if (seen != _graph._nodes.Length)
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
            private int ComputeInitialValues()
            {
                FlowNode[] nodes = _graph._nodes;
                int[] nodeArcs = _graph._nodeArcs;
                FlowArc[] arcs = _graph._arcs;
                NodeState[] nodeStates = _nodeStates;
                ArcState[] arcStates = _arcStates;
                Stack<(int nodeIndex, int nodeArcIndex)> stack = new Stack<(int nodeIndex, int nodeArcIndex)>();

                int tag = ++_tag;

                int seen = 1;
                stack.Push((_rootNode, 0));
                nodeStates[_rootNode].Tag = tag;
                nodeStates[_rootNode].ParentArcIndex = -1;

                while (stack.Count > 0)
                {
                    var (nodeIndex, nodeArcIndex) = stack.Peek();
                    FlowNode node = nodes[nodeIndex];
                    ref NodeState nodeState = ref nodeStates[nodeIndex];

                    if (nodeArcIndex == 0)
                    {
                        nodeState.Supply = node.Supply;
                    }
                    else
                    {
                        // Done with previous child. Update values for it.
                        int arcIndex = nodeArcs[node.ArcsIndex + nodeArcIndex - 1];
                        FlowArc arc = arcs[arcIndex];
                        ref ArcState arcState = ref arcStates[arcIndex];

                        // If outgoing then we must send negative amount of flow to make sum 0
                        // If incoming then we can just send flow along the edge to make it 0
                        double supply;
                        if (arc.Source == nodeIndex)
                            arcState.Value = -(supply = nodeStates[arc.Target].Supply);
                        else
                            arcState.Value = supply = nodeStates[arc.Source].Supply;

                        // In any case we move the supply from neighbor to root
                        nodeState.Supply += supply;
                    }

                    for (; nodeArcIndex < node.CountIn + node.CountOut; nodeArcIndex++)
                    {
                        int arcIndex = nodeArcs[node.ArcsIndex + nodeArcIndex];
                        FlowArc arc = arcs[arcIndex];
                        ref ArcState arcState = ref arcStates[arcIndex];

                        int neiIndex = arc.Source == nodeIndex ? arc.Target : arc.Source;
                        ref NodeState neiState = ref nodeStates[neiIndex];
                        if (neiState.Tag == tag)
                        {
                            // Non-tree edge. Compute slack.
                            // SlackIJ = dualI + costIJ - dualJ
                            if (arc.Source == nodeIndex)
                                arcState.Value = nodeState.DualValue + arc.Cost - neiState.DualValue;
                            else
                                arcState.Value = neiState.DualValue + arc.Cost - nodeState.DualValue;

                            continue;
                        }

                        seen++;
                        neiState.Tag = tag;
                        neiState.ParentArcIndex = arcIndex;
                        arcState.IsTree = true;

                        // Dual feasibility condition states, for edge (i, j):
                        // dualJ - dualI = costIJ
                        if (arc.Source == nodeIndex)
                            neiState.DualValue = arc.Cost + nodeState.DualValue;
                        else
                            neiState.DualValue = nodeState.DualValue - arc.Cost;

                        // Resume from next when we get back to this node
                        stack.Pop();
                        stack.Push((nodeIndex, nodeArcIndex + 1));
                        stack.Push((neiIndex, 0));
                        break;
                    }

                    if (nodeArcIndex == node.CountIn + node.CountOut)
                        stack.Pop();
                }

                return seen;
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

                FindCycle(enteringArcIndex);

                int leavingArcIndex = -1;
                double flow = double.MaxValue;

                int prev = enteringArc.Target;
                foreach (int arcIndex in _cycle)
                {
                    FlowArc arc = arcs[arcIndex];
                    if (arc.Source == prev)
                    {
                        prev = arc.Target;
                        continue; // Same direction as prev.
                    }

                    prev = arc.Source;
                    ref ArcState arcState = ref arcStates[arcIndex];
                    if (arcState.Value < flow)
                    {
                        flow = arcState.Value;
                        leavingArcIndex = arcIndex;
                    }
                    else if (arcState.Value == flow)
                        leavingArcIndex = Math.Min(leavingArcIndex, arcIndex);
                }

                if (leavingArcIndex == -1)
                    return false;

                bool sourceIsSmallest = TagSubTree(leavingArcIndex);
                UpdateTreeForCycle(enteringArcIndex, leavingArcIndex, flow, sourceIsSmallest);
                return true;
            }

            /// <summary>
            /// Finds the cycle that would result when adding the specified arc to the tree.
            /// The cycle is stored in _cycle in the same direction as the specified arc.
            /// The first arc starts at the target of the entering arc.
            /// </summary>
            private void FindCycle(int enteringArcIndex)
            {
                NodeState[] nodeStates = _nodeStates;
                FlowArc[] arcs = _graph._arcs;
                ArcState[] arcStates = _arcStates;
                List<int> cycle = _cycle;

                cycle.Clear();
                int tag = ++_tag;

                FlowArc enteringArc = arcs[enteringArcIndex];
                // Tag from source up to root node.
                int cur = enteringArc.Source;
                while (true)
                {
                    ref NodeState nodeState = ref nodeStates[cur];
                    nodeState.Tag = tag;
                    int parArcIndex = nodeStates[cur].ParentArcIndex;
                    if (parArcIndex == -1)
                        break;
                    FlowArc parArc = arcs[parArcIndex];
                    cur = parArc.Source == cur ? parArc.Target : parArc.Source;
                }

                // Walk from target until we get to a tagged node. This is the common ancestor,
                // so this divides the cycle.
                cur = enteringArc.Target;
                while (true)
                {
                    ref NodeState nodeState = ref nodeStates[cur];
                    if (nodeState.Tag == tag)
                        break;

                    int parArcIndex = nodeStates[cur].ParentArcIndex;
                    cycle.Add(parArcIndex);
                    FlowArc parArc = arcs[parArcIndex];
                    cur = parArc.Source == cur ? parArc.Target : parArc.Source;
                }

                int meetUp = cur;
                int reverseIndex = cycle.Count;
                // Now walk up from source until we get to the meetup point.
                cur = enteringArc.Source;
                while (cur != meetUp)
                {
                    int parArcIndex = nodeStates[cur].ParentArcIndex;
                    cycle.Add(parArcIndex);
                    FlowArc parArc = arcs[parArcIndex];
                    cur = parArc.Source == cur ? parArc.Target : parArc.Source;
                }

                cycle.Reverse(reverseIndex, cycle.Count - reverseIndex);
            }

            /// <summary>
            /// Simulates removing the specified arc and then tags the sub-tree containing the source node.
            /// Returns true if the sub-tree containing the source is the smallest one.
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

                    for (int i = node.ArcsIndex, j = node.ArcsIndex + node.CountIn + node.CountOut; i < j; i++)
                    {
                        int neiArcIndex = nodeArcs[i];
                        ref ArcState neiArcState = ref arcStates[neiArcIndex];

                        if (!neiArcState.IsTree)
                            continue;

                        FlowArc neiArc = arcs[neiArcIndex];
                        int neiNodeIndex = neiArc.Source == nodeIndex ? neiArc.Target : neiArc.Source;
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
            /// This method expects the cycle containing the entering arc to be stored in _cycle.
            /// It also expects the leaving node source sub-tree to be tagged.
            /// The specified flow is pushed along the cycle before the arcs are added and removed.
            /// This method also updates parent indices for correctness. This is done by running DFS
            /// in the non-root sub-tree starting at the entering arc node on this side.
            /// </summary>
            private bool UpdateTreeForCycle(int enteringArcIndex, int leavingArcIndex, double flow, bool sourceIsSmallest)
            {
                FlowNode[] nodes = _graph._nodes;
                int[] nodeArcs = _graph._nodeArcs;
                FlowArc[] arcs = _graph._arcs;
                NodeState[] nodeStates = _nodeStates;
                ArcState[] arcStates = _arcStates;
                Stack<int> stack = _stack;
                List<int> cycle = _cycle;

                FlowArc enteringArc = arcs[enteringArcIndex];
                ref ArcState enteringArcState = ref arcStates[enteringArcIndex];
                FlowArc leavingArc = arcs[leavingArcIndex];
                ref ArcState leavingArcState = ref arcStates[leavingArcIndex];

                // Update primal flows.
                int prev = enteringArc.Target;
                foreach (int arcIndex in _cycle)
                {
                    FlowArc arc = arcs[arcIndex];
                    ref ArcState arcState = ref arcStates[arcIndex];
                    if (arc.Source == prev)
                    {
                        prev = arc.Target;
                        arcState.Value += flow;
                    }
                    else
                    {
                        prev = arc.Source;
                        arcState.Value -= flow;
                    }
                }

                // Disconnect the sub trees.
                leavingArcState.IsTree = false;
                leavingArcState.Value = 0;

                // Update dual slacks for crossing non-tree edges
                int tag = ++_tag;

                int subTreeTag = nodeStates[leavingArc.Source].SubTreeTag;
                if (sourceIsSmallest && nodeStates[_rootNode].SubTreeTag == subTreeTag)
                {
                    // Root is inside smallest sub-tree. Any node in larger sub-tree will have
                    // leaving arc nodes as ancestor, so use leaving arc node in larger sub-tree
                    // as new root.
                    _rootNode = leavingArc.Target;
                }
                else if (!sourceIsSmallest && nodeStates[_rootNode].SubTreeTag != subTreeTag)
                {
                    // Same as above, but the leaving arc node in larger sub-tree is the source.
                    _rootNode = leavingArc.Source;
                }

                // In other cases the root is inside the largest sub tree, so it does not need to change.
                // In all cases we need to link all nodes in the smaller sub tree up to the entering arc.
                int startNodeIndex;
                double startInSubTreeIncrease;
                if (sourceIsSmallest == (nodeStates[enteringArc.Source].SubTreeTag == subTreeTag))
                {
                    // Source node of entering arc is in smallest sub-tree.
                    // "The dual slacks corresponding to those arcs that bridge in the same direction
                    //  as the entering arc get decremented by the old dual slack on the entering arc,
                    //  whereas those that correspond to arcs bridging in the opposite direction get
                    //  incremented by this amount."
                    startNodeIndex = enteringArc.Source;
                    startInSubTreeIncrease = -enteringArcState.Value;
                }
                else
                {
                    startNodeIndex = enteringArc.Target;
                    startInSubTreeIncrease = enteringArcState.Value;
                }

                stack.Clear();
                stack.Push(startNodeIndex);
                nodeStates[startNodeIndex].Tag = tag;
                nodeStates[startNodeIndex].ParentArcIndex = enteringArcIndex;
                nodeStates[_rootNode].ParentArcIndex = -1;

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
                        neiState.ParentArcIndex = arcIndex;
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

                int subTreeTag = nodeStates[leavingArc.Source].SubTreeTag;
                // Find entering arc index. It must bridge the subtrees in opposite direction
                // and have the smallest dual slack.
                leavingArcState.IsTree = false;
                int tag = ++_tag;

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
                            // in opposite order of leaving arc.
                            // The leaving arc always starts in the source, so these arcs should end in the source
                            bool startsInSource = nodeStates[arc.Source].SubTreeTag == subTreeTag;
                            bool endsInSource = nodeStates[arc.Target].SubTreeTag == subTreeTag;
                            if (endsInSource && startsInSource != endsInSource)
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

                FindCycle(enteringArcIndex);
                UpdateTreeForCycle(enteringArcIndex, leavingArcIndex, -leavingArcState.Value, sourceIsSmallest);
                return true;
            }

            [StructLayout(LayoutKind.Auto)]
            private struct NodeState
            {
                public double Supply { get; set; }
                public double DualValue { get; set; }
                public int Tag { get; set; }
                public int SubTreeTag { get; set; }
                public int ParentArcIndex { get; set; }
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
