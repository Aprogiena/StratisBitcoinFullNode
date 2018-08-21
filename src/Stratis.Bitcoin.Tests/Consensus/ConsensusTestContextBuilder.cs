﻿using System;

namespace Stratis.Bitcoin.Tests.Consensus
{
    public sealed class TestContextBuilder
    {
        private readonly TestContext testContext;
        private bool blockstoreAvailable = true;

        public TestContextBuilder()
        {
            this.testContext = new TestContext();
        }

        internal TestContextBuilder WithInitialChain(int initialChainSize, bool assignBlocks = true)
        {
            if (initialChainSize < 0)
                throw new ArgumentOutOfRangeException(nameof(initialChainSize), "Size cannot be less than 0.");

            this.testContext.InitialChainTip = this.testContext.ExtendAChain(initialChainSize, assignBlocks: assignBlocks);
            return this;
        }

        internal TestContextBuilder UseCheckpoints(bool useCheckpoints = true)
        {
            this.testContext.ConsensusSettings.UseCheckpoints = useCheckpoints;
            return this;
        }

        internal TestContextBuilder WithBlockStoreDisabled()
        {
            this.blockstoreAvailable = false;
            return this;
        }

        internal TestContext Build()
        {
            if (this.testContext.InitialChainTip != null)
                this.testContext.ChainedHeaderTree.Initialize(this.testContext.InitialChainTip, this.blockstoreAvailable);

            return this.testContext;
        }

        internal TestContext BuildOnly()
        {
            return this.testContext;
        }
    }
}