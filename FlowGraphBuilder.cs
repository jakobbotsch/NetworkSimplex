using System;
using System.Collections.Generic;
using System.Linq;

namespace NetworkSimplex
{
    public class FlowGraphBuilder
    {
        private readonly List<FlowNodeBuilder> _nodes = new List<FlowNodeBuilder>();
        private readonly List<FlowArcBuilder> _arcs = new List<FlowArcBuilder>();

        public IReadOnlyList<FlowNodeBuilder> Nodes => _nodes;
        public IReadOnlyList<FlowArcBuilder> Arcs => _arcs;

        public FlowNodeBuilder AddNode(double balance = 0)
        {
            var node = new FlowNodeBuilder { Balance = balance };
            _nodes.Add(node);
            return node;
        }

        public void RemoveNode(FlowNodeBuilder node)
        {
            if (node == null)
                throw new ArgumentNullException(nameof(node));

            if (!_nodes.Remove(node))
                throw new ArgumentException("Node is not a member of this graph", nameof(node));
        }

        public FlowArcBuilder AddArc(
            FlowNodeBuilder source = null,
            FlowNodeBuilder target = null,
            double cost = 0)
        {
            var arc = new FlowArcBuilder
            {
                Source = source,
                Target = target,
                Cost = cost,
            };

            _arcs.Add(arc);
            return arc;
        }

        public void RemoveArc(FlowArcBuilder arc)
        {
            if (arc == null)
                throw new ArgumentNullException(nameof(arc));

            if (!_arcs.Remove(arc))
                throw new ArgumentException("Arc is not a member of this graph", nameof(arc));
        }

        public FlowGraph Build()
        {
            foreach (FlowArcBuilder arc in _arcs)
            {
                if (arc.Source == null || arc.Target == null)
                    throw new InvalidOperationException("Not all arcs are connected");
            }

            if (Math.Abs(_nodes.Sum(n => n.Balance)) > 0.00001)
                throw new InvalidOperationException("The balance constraint is not satisfied");

            foreach (var arc in _arcs)
            {
                arc.Source.Index = -1;
                arc.Target.Index = -1;
            }

            for (int i = 0; i < _nodes.Count; i++)
                _nodes[i].Index = i;

            IEnumerable<FlowArc> GetArcs()
            {
                foreach (FlowArcBuilder arc in _arcs)
                {
                    if (arc.Source.Index == -1 ||
                        arc.Target.Index == -1)
                        throw new InvalidOperationException("Some arcs have source/target nodes from other graphs");

                    yield return new FlowArc(arc.Source.Index, arc.Target.Index, arc.Cost);
                }
            }

            return new FlowGraph(
                _nodes.Select(n => n.Balance),
                GetArcs());
        }
    }
    
    public class FlowNodeBuilder
    {
        internal FlowNodeBuilder()
        {
        }

        internal int Index { get; set; }
        public double Balance { get; set; }
    }

    public class FlowArcBuilder
    {
        internal FlowArcBuilder()
        {
        }

        public FlowNodeBuilder Source { get; set; }
        public FlowNodeBuilder Target { get; set; }
        public double Cost { get; set; }
    }
}
