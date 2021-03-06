﻿using LpSolveDotNet;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;

namespace NetworkSimplex
{
    internal class Program
    {
        internal static void Main(string[] args)
        {
            const string testGraphDot = @"digraph G {
  forcelabels=true;

  a;
  b;
  c [label = -6];
  d [label = -6];
  e [label = -2];
  f [label = 9];
  g [label = 5];

  a -> c [label=48];
  a -> d [label=28];
  a -> e [label=10];

  b -> a [label=7];
  b -> c [label=65];
  b -> e [label=7];

  d -> b [label=38];
  d -> e [label=15];

  f -> a [label=56];
  f -> b [label=48];
  f -> c [label=108];
  f -> g [label=24];

  g -> b [label=33];
  g -> e [label=19];
}";

            string testGraph = @"
a 0
b 0
c -6
d -6
e -2
f 9
g 5

a -> d 28
b -> c 65
f -> a 56
f -> b 48
g -> b 33
g -> e 19

a -> c 48
a -> e 10
b -> a 7
b -> e 7
d -> b 38
d -> e 15
f -> c 108
f -> g 24
";

            Test();
            FlowGraph graph = Build(testGraph);
            FlowGraphSolution solution = graph.Solve();

            if (solution.Type == SolutionType.Feasible)
            {
                Console.WriteLine("Optimal solution found!");
                double cost = graph.Arcs.Select((a, i) => a.Cost * solution.Flows[i]).Sum();
                Console.WriteLine("Cost: {0}", cost);
                for (int i = 0; i < graph.Arcs.Count; i++)
                {
                    FlowArc arc = graph.Arcs[i];
                    if (solution.Flows[i] > 0)
                        Console.WriteLine("{0} -> {1} flows {2}", (char)('a' + arc.Source), (char)('a' + arc.Target), solution.Flows[i]);
                }
            }
            else if (solution.Type == SolutionType.Infeasible)
                Console.WriteLine("Network is infeasible");
            else
                Console.WriteLine("Network is unbounded");

            Console.WriteLine("LP program:");
            Console.WriteLine(MakeLpProgram(graph));
            Console.ReadLine();
        }

        private static void Test()
        {
            LpSolve.Init();

            int seed = 1338;
            Console.WriteLine("Seed: {0}", seed);
            Random rand = new Random(seed);
            while (true)
            {
                int numNodes = rand.Next(1000, 5000);
                FlowGraphBuilder builder = new FlowGraphBuilder();
                for (int i = 0; i < numNodes; i++)
                    builder.AddNode();

                for (int i = 0; i < numNodes; i++)
                {
                    int numArcs = rand.Next(1, 15);
                    for (int j = 0; j < numArcs; j++)
                    {
                        while (true)
                        {
                            int target = rand.Next(numNodes);
                            if (target == i)
                                continue;

                            builder.AddArc(builder.Nodes[i], builder.Nodes[target]);
                            break;
                        }
                    }
                }

                foreach (FlowNodeBuilder node in builder.Nodes.Skip(1))
                {
                    node.Supply = rand.Next(-4, -4 + 500);
                    builder.Nodes[0].Supply -= node.Supply;
                }

                foreach (FlowArcBuilder arc in builder.Arcs)
                {
                    arc.Cost = -2 + (rand.NextDouble() * 60);
                }

                FlowGraph graph = builder.Build();
                Stopwatch timer = Stopwatch.StartNew();
                FlowGraphSolution solution = graph.Solve();
                TimeSpan netSimpElapsed = timer.Elapsed;
                double cost = solution.Flows.Select((f, i) => f * graph.Arcs[i].Cost).Sum();
                //(SolutionType lpType, double lpCost, int lpIter, TimeSpan lpElapsed) = SolveLPSolve(graph);
                (SolutionType lpType, double lpCost, int lpIter, TimeSpan lpElapsed) = (solution.Type, cost, 0, TimeSpan.Zero);

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Solved {0}", graph, netSimpElapsed.TotalSeconds);
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("{0,-16} {1,6:F2}s, {2,6} iters", "Network Simplex:", netSimpElapsed.TotalSeconds, solution.NumIterations);
                Console.WriteLine("{0,-16} {1,6:F2}s, {2,6} iters", "LPSolve:", lpElapsed.TotalSeconds, lpIter);
                Console.ForegroundColor = ConsoleColor.Yellow;

                switch (solution.Type)
                {
                    case SolutionType.Feasible:
                        foreach (var node in graph.Nodes)
                        {
                            double supplyLeft = node.Supply;
                            foreach (int outgoing in graph.GetOutgoingArcs(node))
                                supplyLeft -= solution.Flows[outgoing];
                            foreach (int incoming in graph.GetIncomingArcs(node))
                                supplyLeft += solution.Flows[incoming];

                            Trace.Assert(Math.Abs(supplyLeft) < 0.0000001);
                        }

                        Trace.Assert(lpType == SolutionType.Feasible && Math.Abs(cost - lpCost) < 0.001);
                        Console.WriteLine("Optimal solution has {0:F1} cost", cost);
                        break;
                    case SolutionType.Infeasible:
                        Trace.Assert(lpType == SolutionType.Infeasible || lpType == SolutionType.Unbounded);
                        Console.WriteLine("Network is infeasible");
                        break;
                    case SolutionType.Unbounded:
                        Trace.Assert(lpType == SolutionType.Infeasible || lpType == SolutionType.Unbounded);
                        Console.WriteLine("Network is unbounded");
                        break;
                }

                Console.WriteLine();
                Console.ResetColor();
            }
        }

        private static (SolutionType type, double cost, int iter, TimeSpan elapsed) SolveLPSolve(FlowGraph graph)
        {
            LpSolve lp = LpSolve.make_lp(graph.Nodes.Count, graph.Arcs.Count);
            lp.set_debug(false);
            lp.set_verbose(0);
            for (int i = 0; i < graph.Arcs.Count; i++)
                Trace.Assert(lp.set_col_name(i, $"a_{graph.Arcs[i].Source}_{graph.Arcs[i].Target}"));

            Trace.Assert(lp.set_add_rowmode(true));
            foreach (FlowNode node in graph.Nodes)
            {
                List<int> cols = new List<int>();
                List<double> coeffs = new List<double>();

                foreach (int outgoing in graph.GetOutgoingArcs(node))
                {
                    cols.Add(outgoing + 1);
                    coeffs.Add(1);
                }

                foreach (int incoming in graph.GetIncomingArcs(node))
                {
                    cols.Add(incoming + 1);
                    coeffs.Add(-1);
                }

                Trace.Assert(
                    lp.add_constraintex(
                        cols.Count,
                        coeffs.ToArray(),
                        cols.ToArray(),
                        lpsolve_constr_types.EQ,
                        node.Supply));
            }
            lp.set_add_rowmode(false);

            Trace.Assert(
                lp.set_obj_fnex(
                    graph.Arcs.Count,
                    graph.Arcs.Select(a => a.Cost).ToArray(),
                    Enumerable.Range(1, graph.Arcs.Count).ToArray()));

            lp.set_minim();
            Stopwatch timer = Stopwatch.StartNew();
            lpsolve_return ret = lp.solve();
            TimeSpan lpElapsed = timer.Elapsed;

            switch (ret)
            {
                case lpsolve_return.OPTIMAL:
                    return (SolutionType.Feasible, lp.get_objective(), (int)lp.get_total_iter(), lpElapsed);
                case lpsolve_return.INFEASIBLE:
                    return (SolutionType.Infeasible, 0, (int)lp.get_total_iter(), lpElapsed);
                case lpsolve_return.UNBOUNDED:
                    return (SolutionType.Unbounded, 0, (int)lp.get_total_iter(), lpElapsed);
                default:
                    throw new Exception();
            }
        }

        private static string MakeLpProgram(FlowGraph graph)
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("min: ");
            sb.Append(string.Join(" + ", graph.Arcs.Where(a => a.Cost != 0).Select(a => FormattableString.Invariant($"{a.Cost}*a_{a.Source}_{a.Target}"))));
            if (graph.Arcs.All(a => a.Cost == 0))
                sb.Append("0");
            sb.AppendLine(";");
            foreach (var node in graph.Nodes)
            {
                StringBuilder supplySb = new StringBuilder();
                foreach (var outgoing in graph.GetOutgoingArcs(node))
                    supplySb.AppendFormat(CultureInfo.InvariantCulture, "a_{0}_{1} + ", graph.Arcs[outgoing].Source, graph.Arcs[outgoing].Target);

                foreach (var incoming in graph.GetIncomingArcs(node))
                    supplySb.AppendFormat(CultureInfo.InvariantCulture, "-a_{0}_{1} + ", graph.Arcs[incoming].Source, graph.Arcs[incoming].Target);

                if (supplySb.Length > 0)
                    sb.AppendFormat("{0} = {1};", supplySb.ToString(0, supplySb.Length - 3), node.Supply);

                sb.AppendLine();
            }

            return sb.ToString(0, sb.Length - Environment.NewLine.Length);
        }

        private static FlowGraph Build(string input)
        {
            IEnumerable<(string[] split, string line)> EnumerateLines()
            {
                foreach (string line in input.Split('\r', '\n'))
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    string[] split = line.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    yield return (split, line);
                }
            }

            bool error = false;
            FlowGraphBuilder graph = new FlowGraphBuilder();
            Dictionary<string, FlowNodeBuilder> nodes = new Dictionary<string, FlowNodeBuilder>(StringComparer.OrdinalIgnoreCase);
            foreach ((string[] split, string line) in EnumerateLines())
            {
                if (split.Length != 4 && split.Length != 2)
                {
                    Console.WriteLine("Error: I do not understand line '{0}'", line);
                    error = true;
                    continue;
                }

                if (split.Length != 2)
                    continue;

                string nodeName = split[0];
                if (nodes.ContainsKey(nodeName))
                {
                    Console.WriteLine("Error: Duplicate entry for node on line '{0}'", line);
                    error = true;
                    continue;
                }

                if (!double.TryParse(split[1], NumberStyles.Float, CultureInfo.InvariantCulture, out double supply))
                {
                    Console.WriteLine("Error: Could not parse supply on line '{0}'", line);
                    error = true;
                    continue;
                }

                nodes.Add(nodeName, graph.AddNode(supply));
            }

            foreach ((string[] split, string line) in EnumerateLines())
            {
                if (split.Length != 4)
                    continue;

                if (split[1] != "->")
                {
                    Console.WriteLine("Error: I do not understand line '{0}'", line);
                    error = true;
                    continue;
                }

                string from = split[0];
                string to = split[2];
                bool CheckNode(string name)
                {
                    if (nodes.ContainsKey(name))
                        return true;

                    Console.WriteLine("Error: No node by name '{0}' (in line '{1}')", name, line);
                    return false;
                }

                // Intentional bitwise or here
                if (!CheckNode(from) | !CheckNode(to))
                {
                    error = true;
                    continue;
                }

                if (!double.TryParse(split[3], NumberStyles.Float, CultureInfo.InvariantCulture, out double cost))
                {
                    Console.WriteLine("Error: Could not parse cost on line '{0}'", line);
                    error = true;
                    continue;
                }

                graph.AddArc(nodes[from], nodes[to], cost);
            }

            return error ? null : graph.Build();
        }
    }
}
