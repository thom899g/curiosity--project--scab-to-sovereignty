#!/usr/bin/env python3
"""
Project Genesis Bootstrap Module
Main entry point for Hardware Evolution Protocol initialization

ARCHITECTURE: This script orchestrates the forensic analysis and protocol
bootstrapping sequence. All state is managed in Firebase for real-time
auditability and distributed access.

SECURITY: No sensitive credentials are hardcoded. All secrets via .env
or Firebase managed secrets.
"""

import os
import sys
import json
import logging
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple, Optional

# Configure logging BEFORE imports to catch early errors
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('genesis_bootstrap.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Add project root to path for module imports
project_root = Path(__file__).parent
sys.path.append(str(project_root))

try:
    import firebase_admin
    from firebase_admin import credentials, firestore, initialize_app
    import pandas as pd
    import numpy as np
    from sklearn.ensemble import IsolationForest
except ImportError as e:
    logger.error(f"Missing critical dependency: {e}")
    logger.info("Run: pip install -r requirements.txt")
    sys.exit(1)

# Import project modules
try:
    from phase1_forensics import ForensicAnalyzer
    from phase2_protocol import HardwareEvolutionProtocol
    from phase3_antifragile import AntiFragileAllocator
    from security_bounty_engine import SecurityBountyEngine
    from utility_engine import UtilityEngine
    from monitoring_dashboard import ProtocolDashboard
except ImportError as e:
    logger.error(f"Project module import failed: {e}")
    logger.info("Ensure all project files are in the same directory")
    sys.exit(1)


class GenesisBootstrapper:
    """Orchestrates the complete bootstrap sequence for Project Genesis"""
    
    def __init__(self, firebase_cred_path: Optional[str] = None):
        """
        Initialize bootstrapper with Firebase connection
        
        Args:
            firebase_cred_path: Path to Firebase service account key.
                               If None, checks default locations.
        """
        self.firebase_initialized = False
        self.cred_path = self._resolve_credential_path(firebase_cred_path)
        self.db = None
        self._initialize_firebase()
        
        # Initialize component engines
        self.forensic_analyzer = ForensicAnalyzer(self.db)
        self.protocol = HardwareEvolutionProtocol(self.db)
        self.allocator = AntiFragileAllocator(self.db)
        self.bounty_engine = SecurityBountyEngine(self.db)
        self.utility_engine = UtilityEngine(self.db)
        self.dashboard = ProtocolDashboard(self.db)
        
        logger.info("GenesisBootstrapper initialized successfully")
    
    def _resolve_credential_path(self, path: Optional[str]) -> str:
        """
        Resolve Firebase credential file path with fallbacks
        
        Returns:
            Path to service account key
        Raises:
            FileNotFoundError: If no credential file found
        """
        possible_paths = []
        
        if path and os.path.exists(path):
            possible_paths.append(path)
        
        # Check default locations
        default_paths = [
            'serviceAccountKey.json',
            'config/serviceAccountKey.json',
            '../serviceAccountKey.json',
            os.path.join(os.path.expanduser('~'), '.firebase', 'serviceAccountKey.json')
        ]
        
        for default_path in default_paths:
            if os.path.exists(default_path):
                possible_paths.append(default_path)
        
        if not possible_paths:
            error_msg = (
                "Firebase service account key not found.\n"
                "Please download from Firebase Console:\n"
                "1. Go to Project Settings > Service Accounts\n"
                "2. Generate New Private Key\n"
                "3. Save as 'serviceAccountKey.json' in project root"
            )
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)
        
        logger.info(f"Using Firebase credential: {possible_paths[0]}")
        return possible_paths[0]
    
    def _initialize_firebase(self) -> None:
        """Initialize Firebase connection with error handling"""
        try:
            if not firebase_admin._apps:
                cred = credentials.Certificate(self.cred_path)
                firebase_admin.initialize_app(cred)
                logger.info("Firebase Admin SDK initialized")
            
            self.db = firestore.client()
            
            # Test connection
            test_doc = self.db.collection('system_health').document('connection_test')
            test_doc.set({
                'tested_at': firestore.SERVER_TIMESTAMP,
                'status': 'connected',
                'project': 'hardware-evolution-protocol'
            })
            
            self.firebase_initialized = True
            logger.info("Firestore connection established and verified")
            
        except Exception as e:
            logger.error(f"Firebase initialization failed: {e}")
            raise
    
    def _load_asset_manifest(self) -> Dict[str, Any]:
        """
        Load or create asset manifest with integrity checks
        
        Returns:
            Dictionary containing asset metadata
        """
        manifest_path = 'asset_manifest.json'
        
        try:
            if os.path.exists(manifest_path):
                with open(manifest_path, 'r') as f:
                    manifest = json.load(f)
                
                # Validate manifest structure
                required_keys = ['discovery_timestamp', 'initial_hash']
                for key in required_keys:
                    if key not in manifest:
                        raise ValueError(f"Manifest missing required key: {key}")
                
                logger.info(f"Loaded existing manifest from {manifest_path}")
                return manifest
            
            else:
                # Create initial manifest
                initial_manifest = {
                    'discovery_timestamp': datetime.utcnow().isoformat(),
                    'initial_hash': hashlib.sha256(b'initial_asset').hexdigest(),  # Placeholder
                    'version': '1.0.0',
                    'created_by': 'genesis_bootstrap',
                    'notes': 'Initial manifest - forensic analysis pending'
                }
                
                with open(manifest_path, 'w') as f:
                    json.dump(initial_manifest, f, indent=2)
                
                logger.info(f"Created new asset manifest at {manifest_path}")
                return initial_manifest
                
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Manifest loading failed: {e}")
            # Create minimal manifest as fallback
            return {
                'discovery_timestamp': datetime.utcnow().isoformat(),
                'initial_hash': 'error_fallback_hash',
                'error': str(e)
            }
    
    def execute_phase1_forensics(self) -> Dict[str, Any]:
        """
        Execute Phase 1: Forensic analysis of shadow asset
        
        Returns:
            Dictionary containing forensic results
        """
        logger.info("=" * 60)
        logger.info("PHASE 1: FORENSIC INSPIRATION & ADVERSARIAL SIMULATION")
        logger.info("=" * 60)
        
        try:
            # Load asset data (placeholder - will be replaced with actual discovery)
            # In production, this would come from external source
            asset_data = self._discover_asset_data()
            
            # Execute parallel forensic analysis
            forensic_results = self.forensic_analyzer.execute_parallel_forensics(asset_data)
            
            # Log results to Firebase
            forensic_doc = self.db.collection('forensics').document('phase1_results')
            forensic_doc.set({
                **forensic_results,
                'completed_at': firestore.SERVER_TIMESTAMP,
                'phase': '1_complete'
            })
            
            # Create security bounty for any high-risk findings
            if forensic_results.get('risk_score', 0) > 7:
                self.bounty_engine.create_bounty(
                    vulnerability_details=f"High-risk forensic finding: {forensic_results.get('summary', 'Unknown')}",
                    reward_amount=1000  # Base reward
                )
            
            logger.info(f"Phase 1 complete. Risk score: {forensic_results.get('risk_score', 'N/A')}")
            return forensic_results
            
        except Exception as e:
            logger.error(f"Phase 1 execution failed: {e}")
            # Log failure but continue to Phase 2 for resilience
            error_result = {
                'error': str(e),
                'risk_score': 10,  # Maximum risk due to failure
                'completed_at': firestore.SERVER_TIMESTAMP,
                'phase': '1_failed'
            }
            self.db.collection('forensics').document('phase1_error').set(error_result)
            return error_result
    
    def execute_phase2_protocol(self, forensic_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute Phase 2: Sovereign utility engine design
        
        Args:
            forensic_results: Output from Phase 1 analysis
            
        Returns:
            Dictionary containing protocol initialization results
        """
        logger.info("=" * 60)
        logger.info("PHASE 2: SOVEREIGN UTILITY ENGINE DESIGN")
        logger.info("=" * 60)
        
        try:
            # Initialize protocol with asset utility function
            utility_function = self._derive_utility_function(forensic_results)
            protocol_results = self.protocol.initialize(utility_function)
            
            # Bootstrap initial liquidity
            bootstrap_results = self.protocol.bootstrap_liquidity()
            
            # Initialize utility engine
            utility_state = self.utility_engine.initialize()
            
            phase2_results = {
                **protocol_results,
                'bootstrap': bootstrap_results,
                'utility_engine': utility_state,
                'forensic_basis': forensic_results.get('summary', 'Unknown'),
                'completed_at': firestore.SERVER_TIMESTAMP,
                'phase': '2_complete'
            }
            
            # Log to Firebase
            self.db.collection('protocol').document('phase2_results').set(phase2_results)
            
            logger.info(f"Phase 2 complete. Protocol TVL: {bootstrap_results.get('initial_tvl', 0)}")
            return phase2_results
            
        except Exception as e:
            logger.error(f"Phase 2 execution failed: {e}")
            error_result = {
                'error': str(e),
                'completed_at': firestore.SERVER_TIMESTAMP,
                'phase': '2_failed'
            }
            self.db.collection('protocol').document('phase2_error').set(error_result)
            return error_result
    
    def execute_phase3_antifragile(self, protocol_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute Phase 3: Anti-fragile deployment architecture
        
        Args:
            protocol_results: Output from Phase 2
            
        Returns:
            Dictionary containing deployment results
        """
        logger.info("=" * 60)
        logger.info("PHASE 3: ANTI-FRAGILE DEPLOYMENT ARCHITECTURE")
        logger.info("=" * 60)
        
        try:
            # Initialize allocator
            allocator_state = self.allocator.initialize()
            
            # Process initial value flow (simulated)
            initial_value = protocol_results.get('initial_tvl', 1000) * 0.01  # 1% of TVL
            allocation_results = self.allocator.process_value_flow(
                amount=initial_value,
                source='protocol_bootstrap'
            )
            
            # Initialize dashboard monitoring
            dashboard_state = self.dashboard.initialize()
            
            phase3_results = {
                'allocator': allocator_state,
                'initial_allocation': allocation_results,
                'dashboard': dashboard_state,
                'completed_at': firestore.SERVER_TIMESTAMP,
                'phase': '3_complete'
            }
            
            # Log to Firebase
            self.db.collection('deployment').document('phase3_results').set(phase3_results)
            
            logger.info(f"Phase 3 complete. Initial allocation: {allocation_results}")
            return phase3_results
            
        except Exception as e:
            logger.error(f"Phase 3 execution failed: {e}")
            error_result = {
                'error': str(e),
                'completed_at': firestore.SERVER_TIMESTAMP,
                'phase': '3_failed'
            }
            self.db.collection('deployment').document('phase3_error').set(error_result)
            return error_result
    
    def _discover_asset_data(self) -> Dict[str, Any]:
        """
        Discover and load shadow asset data
        
        NOTE: This is a placeholder. In production, this would:
        1. Scan filesystem for asset patterns
        2. Query external APIs
        3. Parse blockchain data if applicable
        4. Validate integrity
        
        Returns:
            Dictionary containing discovered asset data
        """
        logger.warning("Using placeholder asset data - implement discovery logic")
        
        # Simulate asset discovery
        return {
            'asset_type': 'shadow_scab_extraction',
            'discovery_time': datetime.utcnow().isoformat(),
            'potential_value': 10000,  # Placeholder
            'data_samples': ['sample1', 'sample2', 'sample3'],
            'integrity_hash': hashlib.sha256(b'shadow_asset_data').hexdigest(),
            'metadata': {
                'origin_hint': 'legacy_system',
                'liquidity_type': 'dormant_stream',
                'access_method': 'api_endpoint'
            }
        }
    
    def _derive_utility_function(self, forensic_results: Dict[str, Any]) -> callable:
        """
        Derive utility function from forensic analysis
        
        Args:
            forensic_results: Output from Phase 1
            
        Returns:
            Callable utility function
        """
        # This is a simplified example
        asset_type = forensic_results.get('asset_type', 'unknown')
        
        if 'crypto' in asset_type.lower():
            def crypto_utility(user_tier: str) -> float:
                """Crypto asset utility function"""
                tiers = {'basic': 1.0, 'premium': 2.0, 'enterprise': 5.0}
                return tiers.get(user_tier, 0.5)
            return crypto_utility
        
        elif 'api' in asset_type.lower():
            def api_utility(request_count: int) -> float:
                """API asset utility function"""
                return min(request_count * 0.1, 10.0)
            return api_utility
        
        else:
            # Default linear utility
            return lambda x: x * 0.05
    
    def run_complete_bootstrap(self) -> Dict[str, Any]:
        """
        Execute complete bootstrap sequence
        
        Returns:
            Dictionary containing results from all phases
        """
        logger.info("=" * 60)
        logger.info("PROJECT GEN