namespace NetworkSimplex
{
    public class FlowGraphSolution
    {
        public FlowGraphSolution(SolutionType type, double[] flows)
        {
            Type = type;
            Flows = flows;
        }

        public SolutionType Type { get; }
        public double[] Flows { get; }
    }

    public enum SolutionType
    {
        Feasible,
        Infeasible,
        Unbounded,
    }
}
