namespace NetworkSimplex
{
    public class FlowGraphSolution
    {
        public FlowGraphSolution(SolutionType type, double[] flows, int numIterations)
        {
            Type = type;
            Flows = flows;
            NumIterations = numIterations;
        }

        public SolutionType Type { get; }
        public double[] Flows { get; }
        public int NumIterations { get; }

        public override string ToString() => $"{Type} solution, {NumIterations} iters";
    }

    public enum SolutionType
    {
        Feasible,
        Infeasible,
        Unbounded,
    }
}
