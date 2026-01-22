# AWS IAM Roles for Mobile Apps - Implementation Guide

## Executive Summary

This guide explains how to eliminate AWS access keys and secret keys from your mobile app by using AWS IAM Roles with temporary credentials. This is the most secure approach recommended by AWS.

**Benefits:**
- ✅ No long-term credentials stored in app or server
- ✅ Temporary credentials with short TTL (1-12 hours)
- ✅ Automatic credential rotation
- ✅ Fine-grained access control
- ✅ AWS Security Token Service (STS) manages everything
- ✅ Credentials expire automatically

---

## Current Architecture (What We Have Now)

```
┌─────────────────┐
│   Mobile App    │
│   (React Native)│
└────────┬────────┘
         │
         │ 1. Fetch credentials (HTTPS)
         ▼
┌─────────────────┐
│ Credential      │
│ Server (Node.js)│
│                 │
│ - Access Key    │ ← Long-term credentials
│ - Secret Key    │   (never expire)
└─────────────────┘
```

**Issues with Current Approach:**
- Access keys are long-term (never expire unless rotated manually)
- Keys stored on server (environment variables)
- If server is compromised, keys are exposed
- Manual credential rotation required

---

## Proposed Architecture (IAM Roles with Amazon Cognito)

```
┌─────────────────┐
│   Mobile App    │
│   (React Native)│
└────────┬────────┘
         │
         │ 1. Authenticate
         ▼
┌─────────────────┐
│ Amazon Cognito  │
│ Identity Pool   │
└────────┬────────┘
         │
         │ 2. Request temporary credentials
         ▼
┌─────────────────┐
│   AWS STS       │
│ (Token Service) │
└────────┬────────┘
         │
         │ 3. Return temporary credentials
         │    (Access Key + Secret + Session Token)
         │    Valid for 1-12 hours
         ▼
┌─────────────────┐
│   Mobile App    │
│ Uses credentials│
│ to access S3    │
└─────────────────┘
```

**Benefits:**
- ✅ No long-term credentials anywhere
- ✅ Credentials auto-expire (1-12 hours)
- ✅ AWS manages everything
- ✅ Fine-grained permissions per user/device

---

## Implementation Options

### Option 1: Amazon Cognito Identity Pools (Recommended)

**Best for:** Mobile apps with user authentication

**How It Works:**
1. User authenticates (Cognito User Pool, Google, Facebook, etc.)
2. App exchanges auth token for AWS credentials via Cognito Identity Pool
3. Cognito calls AWS STS to get temporary credentials
4. App uses temporary credentials to access S3
5. Credentials expire automatically (default: 1 hour)

**Pros:**
- Easy to implement
- Supports multiple identity providers (Google, Facebook, custom)
- Built-in user management
- Automatic credential rotation
- Free tier available (50,000 MAUs)

**Cons:**
- Requires AWS Cognito setup
- Adds dependency on AWS service

---

### Option 2: Custom Backend with AWS STS (Alternative)

**Best for:** Apps with existing authentication system

**How It Works:**
1. User authenticates with your backend
2. Backend calls AWS STS `AssumeRole` API
3. Backend returns temporary credentials to app
4. App uses credentials to access S3
5. Credentials expire automatically

**Pros:**
- Use existing authentication system
- More control over authentication flow
- Can implement custom authorization logic

**Cons:**
- More complex to implement
- Backend must call AWS STS API
- Need to manage session tokens

---

### Option 3: Web Identity Federation (For Social Sign-In)

**Best for:** Apps using Google/Facebook/Apple sign-in

**How It Works:**
1. User signs in with Google/Facebook/Apple
2. App gets identity token from provider
3. App exchanges token with AWS STS `AssumeRoleWithWebIdentity`
4. AWS STS returns temporary credentials
5. App uses credentials to access S3

**Pros:**
- No backend required for credential vending
- Direct integration with AWS STS
- Simple for social login apps

**Cons:**
- Only works with supported identity providers
- Less control over permissions per user

---

## Detailed Implementation: Option 1 (Amazon Cognito)

### Step 1: AWS Setup

#### 1.1 Create Cognito User Pool (Optional - for user authentication)

```bash
# Using AWS CLI
aws cognito-idp create-user-pool \
  --pool-name "ZivaAppUsers" \
  --policies "PasswordPolicy={MinimumLength=8,RequireUppercase=true,RequireLowercase=true,RequireNumbers=true}" \
  --auto-verified-attributes email
```

Or in AWS Console:
1. Go to Amazon Cognito → User Pools
2. Click "Create user pool"
3. Configure sign-in options (email, phone, username)
4. Configure password policy
5. Note the User Pool ID and App Client ID

#### 1.2 Create Cognito Identity Pool

```bash
# Using AWS CLI
aws cognito-identity create-identity-pool \
  --identity-pool-name "ZivaAppIdentityPool" \
  --allow-unauthenticated-identities \
  --cognito-identity-providers \
    ProviderName="cognito-idp.us-west-2.amazonaws.com/YOUR_USER_POOL_ID",ClientId="YOUR_APP_CLIENT_ID"
```

Or in AWS Console:
1. Go to Amazon Cognito → Identity Pools
2. Click "Create identity pool"
3. Enter identity pool name: `ZivaAppIdentityPool`
4. Choose authentication providers:
   - **Cognito User Pool** (if using Cognito authentication)
   - **Google/Facebook** (if using social login)
   - **Unauthenticated access** (if allowing guest users)
5. Note the Identity Pool ID

#### 1.3 Create IAM Role for S3 Access

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "cognito-identity.amazonaws.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "cognito-identity.amazonaws.com:aud": "us-west-2:YOUR_IDENTITY_POOL_ID"
        },
        "ForAnyValue:StringLike": {
          "cognito-identity.amazonaws.com:amr": "authenticated"
        }
      }
    }
  ]
}
```

Attach this policy to the role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::zivaoneapp/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::zivaoneapp"
    }
  ]
}
```

#### 1.4 Configure Identity Pool with IAM Role

1. Go to Cognito Identity Pool → Edit identity pool
2. Set "Authenticated role" to the IAM role created above
3. Save changes

---

### Step 2: Mobile App Implementation

#### 2.1 Install AWS SDK for JavaScript

```bash
cd /home/ahmed/mongrov/rn-timon
npm install @aws-sdk/client-cognito-identity @aws-sdk/client-s3 @aws-sdk/credential-provider-cognito-identity
```

#### 2.2 Update `package.json`

```json
{
  "dependencies": {
    "@aws-sdk/client-cognito-identity": "^3.400.0",
    "@aws-sdk/client-s3": "^3.400.0",
    "@aws-sdk/credential-provider-cognito-identity": "^3.400.0",
    "react": "18.3.1",
    "react-native": "0.75.4"
  }
}
```

#### 2.3 Create Cognito Helper Module

Create `rn-timon/cognito-auth.ts`:

```typescript
import { CognitoIdentityClient, GetIdCommand, GetCredentialsForIdentityCommand } from '@aws-sdk/client-cognito-identity';

const REGION = 'us-west-2';
const IDENTITY_POOL_ID = 'us-west-2:YOUR_IDENTITY_POOL_ID';

/**
 * Get temporary AWS credentials from Cognito Identity Pool
 * @param idToken - Optional ID token from Cognito User Pool or other identity provider
 * @returns Temporary AWS credentials (AccessKeyId, SecretKey, SessionToken)
 */
export async function getTemporaryCredentials(idToken?: string) {
  try {
    const client = new CognitoIdentityClient({ region: REGION });

    // Step 1: Get Identity ID
    const getIdParams: any = {
      IdentityPoolId: IDENTITY_POOL_ID,
    };

    // If user is authenticated, provide the token
    if (idToken) {
      getIdParams.Logins = {
        [`cognito-idp.${REGION}.amazonaws.com/YOUR_USER_POOL_ID`]: idToken,
      };
    }

    const getIdCommand = new GetIdCommand(getIdParams);
    const identityResponse = await client.send(getIdCommand);
    const identityId = identityResponse.IdentityId;

    if (!identityId) {
      throw new Error('Failed to get identity ID from Cognito');
    }

    console.log('✅ Got Identity ID:', identityId);

    // Step 2: Get temporary credentials
    const getCredsParams: any = {
      IdentityId: identityId,
    };

    if (idToken) {
      getCredsParams.Logins = {
        [`cognito-idp.${REGION}.amazonaws.com/YOUR_USER_POOL_ID`]: idToken,
      };
    }

    const getCredsCommand = new GetCredentialsForIdentityCommand(getCredsParams);
    const credentialsResponse = await client.send(getCredsCommand);

    const credentials = credentialsResponse.Credentials;

    if (!credentials || !credentials.AccessKeyId || !credentials.SecretKey || !credentials.SessionToken) {
      throw new Error('Failed to get credentials from Cognito');
    }

    console.log('✅ Got temporary credentials');
    console.log('   Expiration:', credentials.Expiration);

    return {
      accessKeyId: credentials.AccessKeyId,
      secretAccessKey: credentials.SecretKey,
      sessionToken: credentials.SessionToken,
      expiration: credentials.Expiration,
    };
  } catch (error: any) {
    console.error('❌ Failed to get temporary credentials:', error);
    throw new Error(`Failed to get temporary credentials: ${error.message}`);
  }
}

/**
 * Check if credentials are expired
 */
export function areCredentialsExpired(expiration: Date): boolean {
  const now = new Date();
  const expirationDate = new Date(expiration);
  // Refresh 5 minutes before expiration
  const bufferTime = 5 * 60 * 1000; // 5 minutes in milliseconds
  return now.getTime() >= (expirationDate.getTime() - bufferTime);
}
```

#### 2.4 Update Rust Native Module to Support Session Token

Modify `rn-timon/timon/src/timon_engine/cloud_sync.rs`:

```rust
use object_store::{aws::AmazonS3Builder, ObjectStore};

pub struct CloudStorageManager {
    pub s3: Arc<dyn ObjectStore>,
}

impl CloudStorageManager {
    /// Create new CloudStorageManager with temporary credentials (includes session token)
    pub fn new_with_session_token(
        bucket_endpoint: String,
        bucket_name: String,
        mut access_key_id: String,
        mut secret_access_key: String,
        mut session_token: String,
        bucket_region: String,
    ) -> Result<Self, TimonError> {
        let s3 = AmazonS3Builder::new()
            .with_endpoint(&bucket_endpoint)
            .with_bucket_name(&bucket_name)
            .with_access_key_id(&access_key_id)
            .with_secret_access_key(&secret_access_key)
            .with_token(&session_token)  // ← Session token for temporary credentials
            .with_region(&bucket_region)
            .build()
            .map_err(|e| TimonError::cloud_sync_error(format!("Failed to build S3 client: {}", e)))?;

        // Zeroize credentials immediately after use
        access_key_id.zeroize();
        secret_access_key.zeroize();
        session_token.zeroize();

        Ok(Self {
            s3: Arc::new(s3),
        })
    }
}
```

#### 2.5 Update `App.tsx` to Use Cognito

```typescript
import React, { useState, useEffect } from 'react';
import { getTemporaryCredentials, areCredentialsExpired } from './cognito-auth';
import { storeCredentialsSecurely, initBucketFromSecureStorage } from './test-rust-module';

// Store credentials with expiration
let cachedCredentials: {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken: string;
  expiration: Date;
} | null = null;

async function initializeBucketWithCognito() {
  try {
    // Check if cached credentials are still valid
    if (cachedCredentials && !areCredentialsExpired(cachedCredentials.expiration)) {
      console.log('✅ Using cached temporary credentials');
      return;
    }

    console.log('📡 Fetching temporary credentials from AWS Cognito...');

    // Get temporary credentials from Cognito
    // If you have a user ID token, pass it here: getTemporaryCredentials(idToken)
    const credentials = await getTemporaryCredentials();

    // Cache credentials
    cachedCredentials = credentials;

    console.log('🔒 Storing temporary credentials securely...');

    // Store in Android Keystore (with session token)
    await storeCredentialsSecurely(
      'https://s3.us-west-2.amazonaws.com',
      'zivaoneapp',
      credentials.accessKeyId,
      credentials.secretAccessKey,
      'us-west-2',
      credentials.sessionToken  // ← Pass session token
    );

    console.log('✅ Bucket initialized with temporary credentials');
    console.log(`   Credentials expire at: ${credentials.expiration}`);

    // Initialize bucket
    await initBucketFromSecureStorage();

  } catch (error: any) {
    console.error('❌ Failed to initialize bucket with Cognito:', error);
    throw error;
  }
}

// Initialize on app start
(async () => {
  try {
    await initializeBucketWithCognito();
  } catch (error) {
    console.error('Failed to initialize:', error);
  }
})();

// Refresh credentials periodically (e.g., every 30 minutes)
setInterval(async () => {
  if (cachedCredentials && areCredentialsExpired(cachedCredentials.expiration)) {
    console.log('🔄 Credentials expired, refreshing...');
    try {
      await initializeBucketWithCognito();
    } catch (error) {
      console.error('Failed to refresh credentials:', error);
    }
  }
}, 30 * 60 * 1000); // Check every 30 minutes
```

---

### Step 3: Update Native Modules

#### 3.1 Update JNI to Accept Session Token

Modify `rn-timon/timon/src/lib.rs`:

```rust
#[no_mangle]
pub extern "system" fn Java_com_rustexample_TimonModule_nativeInitBucketWithSessionToken(
    mut env: JNIEnv,
    _class: JClass,
    bucket_endpoint: JString,
    bucket_name: JString,
    access_key_id: JString,
    secret_access_key: JString,
    session_token: JString,
    bucket_region: JString,
) -> jstring {
    // Security checks
    if let Err(e) = security::perform_security_checks() {
        let error_msg = format!("{{\"status\": 500, \"message\": \"Security error: {}\"}}", e);
        return env.new_string(error_msg).unwrap().into_raw();
    }

    // Convert JStrings to Rust Strings
    let mut rust_access_key_id: String = env.get_string(&access_key_id).unwrap().into();
    let mut rust_secret_access_key: String = env.get_string(&secret_access_key).unwrap().into();
    let mut rust_session_token: String = env.get_string(&session_token).unwrap().into();
    let rust_bucket_endpoint: String = env.get_string(&bucket_endpoint).unwrap().into();
    let rust_bucket_name: String = env.get_string(&bucket_name).unwrap().into();
    let rust_bucket_region: String = env.get_string(&bucket_region).unwrap().into();

    // Initialize bucket with session token
    match init_bucket_with_session_token(
        rust_bucket_endpoint,
        rust_bucket_name,
        rust_access_key_id.clone(),
        rust_secret_access_key.clone(),
        rust_session_token.clone(),
        rust_bucket_region,
    ) {
        Ok(_) => {
            // Zeroize credentials
            rust_access_key_id.zeroize();
            rust_secret_access_key.zeroize();
            rust_session_token.zeroize();

            let success_msg = "{\"status\": 200, \"message\": \"Bucket initialized with temporary credentials\"}";
            env.new_string(success_msg).unwrap().into_raw()
        }
        Err(e) => {
            // Zeroize credentials even on error
            rust_access_key_id.zeroize();
            rust_secret_access_key.zeroize();
            rust_session_token.zeroize();

            let error_msg = format!("{{\"status\": 500, \"message\": \"Failed to init bucket: {}\"}}", e);
            env.new_string(error_msg).unwrap().into_raw()
        }
    }
}
```

#### 3.2 Update Kotlin Module

Modify `rn-timon/android/app/src/main/java/com/rustexample/TimonModule.kt`:

```kotlin
@ReactMethod
fun initBucketWithSessionToken(
    bucketEndpoint: String,
    bucketName: String,
    accessKeyId: String,
    secretAccessKey: String,
    sessionToken: String,
    bucketRegion: String,
    promise: Promise
) {
    try {
        val result = nativeInitBucketWithSessionToken(
            bucketEndpoint,
            bucketName,
            accessKeyId,
            secretAccessKey,
            sessionToken,
            bucketRegion
        )
        promise.resolve(result)
    } catch (e: Exception) {
        promise.reject("INIT_BUCKET_ERROR", "Failed to initialize bucket: ${e.message}", e)
    }
}

// Native method declaration
private external fun nativeInitBucketWithSessionToken(
    bucketEndpoint: String,
    bucketName: String,
    accessKeyId: String,
    secretAccessKey: String,
    sessionToken: String,
    bucketRegion: String
): String
```

---

## Security Comparison

| Feature | Long-Term Keys | Temporary Credentials (IAM Roles) |
|---------|----------------|-----------------------------------|
| **Credential Lifetime** | Never expire (manual rotation) | 1-12 hours (auto-expire) |
| **Storage** | Server environment variables | In-memory only (refreshed) |
| **Rotation** | Manual | Automatic |
| **Compromise Risk** | High (keys never expire) | Low (keys expire quickly) |
| **Revocation** | Manual (delete keys) | Automatic (expire) |
| **Audit Trail** | CloudTrail (per key) | CloudTrail (per user/session) |
| **Implementation** | Simple | Medium complexity |
| **Cost** | Free | Free (Cognito: 50K MAUs free) |

---

## Cost Analysis

### Amazon Cognito Pricing (as of 2026)

**Identity Pools:**
- First 50,000 MAUs (Monthly Active Users): **FREE**
- Next 50,000 MAUs: $0.0055 per MAU
- Above 100,000 MAUs: $0.0046 per MAU

**User Pools:**
- First 50,000 MAUs: **FREE**
- Next 50,000 MAUs: $0.0055 per MAU

**AWS STS (Security Token Service):**
- **FREE** for all API calls

**Example:**
- 10,000 users: **$0/month** (within free tier)
- 100,000 users: **$275/month** (50K free + 50K × $0.0055)
- 1,000,000 users: **$4,875/month**

---

## Migration Path

### Phase 1: Parallel Implementation (Weeks 1-2)
1. Set up Cognito Identity Pool
2. Create IAM roles
3. Implement Cognito authentication in app (parallel to existing)
4. Test with temporary credentials

### Phase 2: Testing (Week 3)
1. Test credential refresh logic
2. Verify S3 access with temporary credentials
3. Load testing
4. Security testing

### Phase 3: Rollout (Week 4)
1. Deploy to staging
2. Gradual rollout (10% → 50% → 100%)
3. Monitor for issues
4. Keep credential server as backup

### Phase 4: Cleanup (Week 5)
1. Remove old credential server
2. Rotate/delete long-term AWS keys
3. Update documentation

---

## Troubleshooting

### Issue: "NotAuthorizedException: User is not authenticated"

**Solution**: Ensure Identity Pool is configured to allow unauthenticated access, or provide a valid ID token.

```typescript
// For authenticated users
const credentials = await getTemporaryCredentials(idToken);

// For unauthenticated (guest) users
const credentials = await getTemporaryCredentials();
```

### Issue: "AccessDeniedException: User is not authorized to perform: s3:GetObject"

**Solution**: Check IAM role permissions. Ensure the role attached to the Identity Pool has S3 permissions.

### Issue: Credentials expire too quickly

**Solution**: Increase session duration in IAM role settings (up to 12 hours for Cognito).

---

## Additional Resources

- [AWS Cognito Documentation](https://docs.aws.amazon.com/cognito/)
- [AWS STS Documentation](https://docs.aws.amazon.com/STS/latest/APIReference/)
- [AWS SDK for JavaScript v3](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/)
- [React Native AWS Amplify](https://docs.amplify.aws/react-native/)

---

## Summary

**Recommended Approach:** Amazon Cognito Identity Pools

**Benefits:**
- ✅ No long-term credentials
- ✅ Automatic credential rotation
- ✅ Temporary credentials (1-12 hours)
- ✅ Fine-grained access control
- ✅ Free for up to 50,000 users

**Implementation Effort:** Medium (2-3 weeks)

**Cost:** Free tier covers most apps (50K MAUs)

---

**Last Updated**: 2026-01-21  
**Status**: Recommended future improvement
