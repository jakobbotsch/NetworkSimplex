using System;
using System.Collections.Generic;
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

a -> c 48
a -> d 28
a -> e 10
b -> a 7
b -> c 65
b -> e 7
d -> b 38
d -> e 15
f -> a 56
f -> b 48
f -> c 108
f -> g 24
g -> b 33
g -> e 19";

            testGraph = @"
a -1
b 1
c -1
d 1

b -> a 3
b -> c -7
d -> a -2
d -> c -9
";

            FlowGraph graph = Build(testGraph);
            FlowGraphSolution solution = graph.Solve();

            double cost = graph.Arcs.Select((a, i) => a.Cost * solution.Flows[i]).Sum();
            Console.WriteLine("Cost: {0}", cost);
            for (int i = 0; i < graph.Arcs.Count; i++)
            {
                FlowArc arc = graph.Arcs[i];
                if (solution.Flows[i] > 0)
                    Console.WriteLine("{0} -> {1} flows {2}", (char)('a' + arc.Source), (char)('a' + arc.Target), solution.Flows[i]);
            }

            Console.WriteLine("LP program:");
            Console.WriteLine("min: {0};", string.Join(" + ", graph.Arcs.Where(a => a.Cost != 0).Select(a => FormattableString.Invariant($"{a.Cost}*a_{a.Source}_{a.Target}"))));
            foreach (var node in graph.Nodes)
            {
                StringBuilder sb = new StringBuilder();
                foreach (var outgoing in graph.GetOutgoingArcs(node))
                    sb.AppendFormat(CultureInfo.InvariantCulture, "a_{0}_{1} + ", graph.Arcs[outgoing].Source, graph.Arcs[outgoing].Target);

                foreach (var incoming in graph.GetIncomingArcs(node))
                    sb.AppendFormat(CultureInfo.InvariantCulture, "-a_{0}_{1} + ", graph.Arcs[incoming].Source, graph.Arcs[incoming].Target);

                if (sb.Length > 0)
                    Console.WriteLine("{0} = {1};", sb.ToString(0, sb.Length - 3), node.Balance);
            }
            Console.ReadLine();
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

                if (!double.TryParse(split[1], NumberStyles.Float, CultureInfo.InvariantCulture, out double balance))
                {
                    Console.WriteLine("Error: Could not parse balance on line '{0}'", line);
                    error = true;
                    continue;
                }

                nodes.Add(nodeName, graph.AddNode(balance));
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
